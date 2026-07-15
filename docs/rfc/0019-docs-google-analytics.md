# RFC 0019 — Google Analytics for the docs site (per-page tracking on a SPA)

- **Status**: ✅ Implemented
- **Target**: `docs/` (the docsify site — `index.html` only)
- **Depends on**: RFC 0018 (the 2.1.0 docs the site now ships)
- **Part of**: docs/site operations, not the engine — no `core`/`reactive` change
- **Realized by**: the **Google tag (gtag.js)** loader for GA4 measurement ID
  `G-YW2QB9NVPV` in `<head>`, configured with `send_page_view: false`, plus a
  docsify `doneEach` plugin that sends one `page_view` per route render — all in
  `docs/index.html`. No new file, no build-time dependency (gtag.js loads from
  `googletagmanager.com` at runtime, like the other CDN scripts already do).

## Summary

The docs site at **nioflow.dev** has no analytics: we cannot tell which pages
readers reach, where they enter, or which of the guide/reference/RFC pages carry
their weight. This RFC adds **Google Analytics 4** (gtag.js, measurement ID
`G-YW2QB9NVPV`) to the site and — the actual requirement — makes it **register a
page view every time the reader moves to a different page**, not just on the
first load.

That "every time" is the whole problem. The site is a **docsify single-page
app**: navigating from `#/quickstart` to `#/webflux` never reloads the document,
so the standard gtag snippet — which reports one `page_view` when it boots — sees
the entry page and nothing after it. This RFC's mechanism is to hook docsify's
own "a page just rendered" signal and send one `page_view` per navigation.

It changes no engine behaviour and touches exactly one file.

## The problem: gtag counts loads, docsify never reloads

`docs/index.html` is a docsify SPA (`window.$docsify`, `#/route` hash routing).
The stock GA4 install fires a single `page_view` at boot:

```html
<script async src="https://www.googletagmanager.com/gtag/js?id=G-YW2QB9NVPV"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){ dataLayer.push(arguments); }
  gtag('js', new Date());
  gtag('config', 'G-YW2QB9NVPV');   // sends ONE page_view, here, on boot
</script>
```

`gtag('config', …)` fires a single `page_view` at boot and then stays silent.
Every later navigation in docsify is a **hash change** that swaps the
`.markdown-section` content in place — no document load, so no second
`page_view`. The result would be: one hit for the landing page, zero for
Quickstart, WebFlux, the API reference or any RFC. That is the opposite of the
requirement.

**GA4 "Enhanced measurement" does not save us here.** Its SPA option tracks
"page changes based on **browser history events**" — `pushState`/`popstate` from
the History API. docsify routes on the **URL fragment** (`#/webflux`); a hash
change does not go through the History API in the way GA4's automatic listener
watches, so enhanced measurement is unreliable for this router. We send the page
view ourselves, from the one event that is always both present and
correctly-timed: docsify telling us it **finished rendering** a route.

## The mechanism: one `page_view` per `doneEach`

docsify's plugin API exposes `hook.doneEach(fn)`, which fires **after every page
render** — the initial one and every navigation after it. `index.html` already
uses it (to restore the navbar, run mermaid, inject the cover copy button), so
the extension point is proven in this file.

The loader turns the automatic page view **off** so the config call does not also
emit the landing hit (which `doneEach` emits on the first render — otherwise the
entry page is counted twice):

```html
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-YW2QB9NVPV"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){ dataLayer.push(arguments); }
  gtag('js', new Date());
  gtag('config', 'G-YW2QB9NVPV', { send_page_view: false });
</script>
<!-- End Google tag -->
```

The plugin reads the current route and title from the docsify view-model and
sends a manual GA4 `page_view`:

```js
// ── Google Analytics: one page_view per docsify route render ──
// gtag('config', …) runs with send_page_view:false, so doneEach owns EVERY
// page_view including the first render — no double count.
function (hook, vm) {
  hook.doneEach(function () {
    if (typeof gtag !== 'function') {
      return;                      // gtag.js blocked (ad-blocker) — no-op, never throw
    }
    var path = '/' + (vm.route.path || '').replace(/^\/+/, '');   // e.g. "/webflux"
    gtag('event', 'page_view', {
      page_path: path,             // the hash route, normalised to a clean path
      page_title: document.title,  // docsify sets <title> per page
      page_location: location.origin + '/#' + path
    });
  });
}
```

Why this shape:

- **`doneEach` owns every `page_view`, `config` owns none.** One producer of page
  views means no way to double-count and no way to miss the first render —
  `doneEach` fires on the initial render too. The alternative (let `config` send
  the first, `doneEach` send the rest) has to special-case the initial render and
  is one refactor away from either a duplicate or a hole.
- **`page_path` is the hash route, normalised.** `vm.route.path` is docsify's own
  idea of the current page (`/webflux`, `/rfc/0019-docs-google-analytics`), which
  is exactly the granularity a reader thinks in — not the raw `location.pathname`,
  which is always `/` on a hash SPA and would collapse every page into one row.
- **It fails closed.** If gtag.js is blocked (ad-blocker, DNT extension, a network
  hiccup) `gtag` is absent; the guard returns and the docs render exactly as
  today. Analytics is never allowed to break a page.

## Configuration

- **The measurement ID (`G-YW2QB9NVPV`) is public by design** — it ships in
  client-side JS on every analytics site and identifies a *destination*, not a
  secret. It is embedded in `index.html`. (The Measurement Protocol *API secret*,
  which is not used here, is the thing that must never ship; we send only through
  gtag.js in the browser.)
- **One property, web stream `nioflow.dev`.** The ID is provisioned in the GA4
  console; the RFC does not choose the account.
- **No build wiring.** gtag.js loads from `googletagmanager.com` at runtime, the
  same pattern as docsify, mermaid and prism already use from jsDelivr — GitHub
  Pages serves a static file, there is nothing to configure server-side.

## Non-goals

- **No engine, `core/` or `reactive/` change.** This is a docs-site file. The
  library ships zero analytics and zero new dependency.
- **No Google Tag Manager.** A GTM container was considered (it moves tag config
  into a console, no redeploy to add tags) but rejected for this: it is a second
  moving part and a second third-party load for a docs site whose only need is
  page views. gtag.js talks to GA4 directly.
- **No custom event taxonomy** (search terms, copy-code clicks, outbound link
  tracking, scroll depth). This RFC delivers the one requirement — page views per
  navigation. Custom events can be a follow-up; they would hang off the same
  `gtag('event', …)` call.
- **No consent banner / CMP in this RFC.** See Risks — this is a deliberate
  scoping decision to call out, not silently skip, because it has legal weight
  depending on audience and region.
- **No server-side or Measurement-Protocol tracking.** Browser gtag.js only.
- **No second analytics vendor**, no self-hosted alternative (Plausible/Umami)
  evaluated here — the request is specifically Google Analytics.

## Testing

The docs have no unit suite; the guardrails are manual and mechanical:

1. **The page-view fires per navigation, not per load.** Open the site with GA4
   **DebugView** (or the `Google Analytics Debugger` extension), and confirm one
   `page_view` on the landing render and **one more on each** move to Quickstart
   → WebFlux → an RFC → back. The `page_path` in each hit matches `vm.route.path`.
2. **No double count on entry.** The landing page produces exactly one
   `page_view`, not two — the check that `send_page_view: false` is actually set.
3. **Blocked-gtag degradation.** Load with gtag.js blocked (uBlock, or DevTools
   request-block on `googletagmanager.com`); the site renders and navigates
   normally, no console error, no thrown exception from the plugin (the guarded
   `typeof gtag` returns).
4. **The existing `doneEach` consumers still run** — navbar restore, mermaid,
   cover copy — i.e. adding a fourth plugin did not disturb the three already in
   `index.html`.

Because the change is a hash-route SPA concern, the load-bearing case is #1: a
reviewer must see the *second* and *third* page views appear without a reload,
which is the exact behaviour the naive install fails to produce.

## Risks

- **Privacy / consent is real and out of this RFC's mechanism.** GA4 sets cookies
  and processes IP-derived data; depending on the audience and jurisdiction
  (GDPR/ePrivacy) a public site may owe a consent gate before loading gtag.js.
  This RFC ships the *tracking*, not the *consent policy* — the decision to add a
  CMP, enable GA's Consent Mode, or rely on IP anonymisation is called out here so
  it is made on purpose. If consent is required, the loader moves behind a consent
  check and `doneEach` still owns the page views once granted. **Whether consent
  is required for nioflow.dev's audience remains an operational decision to close,
  independent of this code.**
- **A hard third-party dependency on `googletagmanager.com`.** gtag.js loads
  `async` and the plugin is failure-tolerant (guarded `typeof gtag`), so a blocked
  or slow gtag.js never delays or breaks a render — but it is one more external
  origin the site talks to, and readers who block it simply go uncounted
  (acceptable; better than a page that breaks).
- **Route/path drift.** If docsify's routing config changes (e.g. History-API mode
  instead of hash), `vm.route.path` stays correct but `page_location` should be
  revisited. The path is read from docsify's own view-model precisely so it tracks
  the router rather than guessing from the URL.
- **Under-counting vs the naive install, by design.** DNT users, ad-blockers and
  no-JS readers are invisible. That is the correct trade for a docs site; we
  optimise for not-breaking over completeness.
