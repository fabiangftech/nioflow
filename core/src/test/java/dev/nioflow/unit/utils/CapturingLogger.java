package dev.nioflow.unit.utils;

import java.lang.System.Logger;
import java.text.MessageFormat;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.CopyOnWriteArrayList;

/** Test double for {@link Logger}: keeps every formatted line. */
public final class CapturingLogger implements Logger {

    public final List<String> lines = new CopyOnWriteArrayList<>();

    @Override
    public String getName() {
        return "capturing";
    }

    @Override
    public boolean isLoggable(Level level) {
        return true;
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String message, Throwable thrown) {
        lines.add(message);
    }

    @Override
    public void log(Level level, ResourceBundle bundle, String format, Object... params) {
        lines.add(MessageFormat.format(format, params));
    }
}
