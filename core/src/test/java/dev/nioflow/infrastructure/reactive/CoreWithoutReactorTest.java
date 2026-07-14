package dev.nioflow.infrastructure.reactive;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Core keeps its promise of zero required runtime dependencies.
 *
 * <p>Reactor is compileOnly, like OpenTelemetry and Resilience4j: a consumer who
 * does not bring reactor-core must still be able to build and run a flow. The
 * only thing standing between that promise and a production NoClassDefFoundError
 * is that NOTHING on the engine's path statically references Reactor — which is
 * easy to break by accident (one convenience overload on NioStep would do it).
 *
 * <p>So this test loads the engine through a classloader that pretends Reactor
 * does not exist, and runs a pipeline in it.
 */
class CoreWithoutReactorTest {

    @Test
    void aFlowRunsWithReactorAbsentFromTheClasspath() throws Exception {
        ClassLoader hidingReactor = new ReactorHidingClassLoader();

        // Loaded fresh, in the hostile loader: if any class it touches mentions
        // reactor.**, resolution fails right here.
        Class<?> probe = Class.forName(Probe.class.getName(), true, hidingReactor);
        Method run = probe.getDeclaredMethod("run");
        Object result = run.invoke(probe.getDeclaredConstructor().newInstance());

        assertEquals(84, result);
        assertEquals(hidingReactor, probe.getClassLoader(), "the probe must not have leaked to the app loader");
    }

    /** Runs a real pipeline. Loaded by the hiding classloader, never by JUnit's. */
    public static class Probe {

        public Integer run() {
            var engine = new dev.nioflow.application.facade.DefaultNioEngine();
            try {
                dev.nioflow.core.facade.NioFlow<Integer, Integer> flow =
                        dev.nioflow.application.facade.DefaultNioFlow.from(Integer.class, engine);
                flow.handle("double", value -> value * 2);
                engine.seal();
                return flow.just(42).execute();
            } finally {
                engine.shutdown(java.time.Duration.ofSeconds(1));
            }
        }
    }

    /**
     * Loads dev.nioflow.** itself (so the engine classes are resolved here, not
     * delegated to the parent) and refuses reactor.** outright.
     */
    private static final class ReactorHidingClassLoader extends ClassLoader {

        private ReactorHidingClassLoader() {
            super(ClassLoader.getSystemClassLoader().getParent());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("reactor.")) {
                throw new ClassNotFoundException("Reactor is hidden: " + name);
            }
            if (!name.startsWith("dev.nioflow.")) {
                return super.loadClass(name, resolve);
            }
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) {
                loaded = defineFromClasspath(name);
            }
            if (resolve) {
                resolveClass(loaded);
            }
            return loaded;
        }

        private Class<?> defineFromClasspath(String name) throws ClassNotFoundException {
            String resource = name.replace('.', '/') + ".class";
            try (var stream = ClassLoader.getSystemResourceAsStream(resource)) {
                if (stream == null) {
                    throw new ClassNotFoundException(name);
                }
                byte[] bytecode = stream.readAllBytes();
                return defineClass(name, bytecode, 0, bytecode.length);
            } catch (java.io.IOException failure) {
                throw new ClassNotFoundException(name, failure);
            }
        }
    }
}
