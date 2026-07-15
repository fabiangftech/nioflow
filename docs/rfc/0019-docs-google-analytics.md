# RFC 0019 — Google Analytics for the docs site (per-page tracking on a SPA)

- **Status**: ✅ Implemented
- **Target**: `docs/` (the docsify site — `index.html` only)
- **Depends on**: RFC 0018 (the 2.1.0 docs the site now ships)
- **Part of**: docs/site operations, not the engine — no `core`/`reactive` change
- **Realized by**: the **Google Tag Manager** container snippet (`GTM-W48ZV6XD`)
  in `<head>` + its `<noscript>` after `<body>`, and a docsify `doneEach` plugin
  that pushes one `spa_page_view` event to the `dataLayer` per route render — all
  in `docs/index.html`. No new file, no build-time dependency (gtm.js loads from
  `googletagmanager.com` at runtime, like the other CDN scripts already do). The
  GA4 page-view tag itself is wired **inside GTM's console**, off the container.

## Summary

The docs site at **nioflow.dev** has no analytics: we cannot tell which pages
readers reach, where they enter, or which of the guide/reference/RFC pages carry
their weight. This RFC adds **Google Analytics 4 via Google Tag Manager**
(container `GTM-W48ZV6XD`) to the site and — the actual requirement — makes it
**register a page view every time the reader moves to a different page**, not
just on the first load.

That "every time" is the whole problem. The site is a **docsify single-page
app**: navigating from `#/quickstart` to `#/webflux` never reloads the document,
so GTM's own page-load hit fires once, on boot, and sees the entry page and
nothing after it. This RFC's mechanism is to hook docsify's own "a page just
rendered" signal and push one page-view event to the `dataLayer` per navigation,
which the GA4 tag inside GTM turns into a `page_view`.

It changes no engine behaviour and touches exactly one file.

## The problem: the container loads once, docsify never reloads

`docs/index.html` is a docsify SPA (`window.$docsify`, `#/route` hash routing).
The GTM container snippet goes in `<head>` (with its `<noscript>` iframe after
`<body>`) and bootstraps the `dataLayer`, then loads gtm.js:

```html
<!-- Google Tag Manager -->
<script>(function(w,d,s,l,i){w[l]=w[l]||[];w[l].push({'gtm.start':
new Date().getTime(),event:'gtm.js'});var f=d.getElementsByTagName(s)[0],
j=d.createElement(s),dl=l!='dataLayer'?'&l='+l:'';j.async=true;j.src=
'https://www.googletagmanager.com/gtm.js?id='+i+dl;f.parentNode.insertBefore(j,f);
})(window,document,'script','dataLayer','GTM-W48ZV6XD');</script>
<!-- End Google Tag Manager -->
```

On its own that fires the container's page-load event **once**, at boot, and
then stays silent. Every later navigation in docsify is a **hash change** that
swaps the `.markdown-section` content in place — no document load, so no second
page view. The result would be: one hit for the landing page, zero for
Quickstart, WebFlux, the API reference or any RFC. That is the opposite of the
requirement.

**Neither GA4 "Enhanced measurement" nor GTM's History Change trigger is the
right signal.** GA4's SPA option watches History-API events
(`pushState`/`popstate`); docsify routes on the **URL fragment** (`#/webflux`),
which does not go through them, so it under-counts. GTM's built-in *History
Change* trigger *does* fire on `hashchange` — but it fires the **instant the hash
changes, before docsify has re-rendered**, so `document.title` (and the DOM the
tag might read) is still the *previous* page's. We instead push the event from
the one signal that is always both present and correctly-timed: docsify telling
us it **finished rendering** a route.

## The mechanism: one `dataLayer` push per `doneEach`

docsify's plugin API exposes `hook.doneEach(fn)`, which fires **after every page
render** — the initial one and every navigation after it. `index.html` already
uses it (to restore the navbar, run mermaid, inject the cover copy button), so
the extension point is proven in this file.

The plugin reads the current route and title from the docsify view-model and
pushes an `spa_page_view` event to the `dataLayer`; the GA4 tag inside GTM is
triggered on that event and turns it into a `page_view`:

```js
// ── Google Tag Manager: one page_view per docsify route render ──
// document.title is final by doneEach, so page_title is the right page.
// GTM config: fire the GA4 page_view tag on this 'spa_page_view' event and
// leave the GA4 config tag's automatic page_view OFF, so the first render
// is counted once, not twice — doneEach owns EVERY page view.
function (hook, vm) {
  hook.doneEach(function () {
    var path = '/' + (vm.route.path || '').replace(/^\/+/, '');   // e.g. "/webflux"
    (window.dataLayer = window.dataLayer || []).push({
      event: 'spa_page_view',
      page_path: path,             // the hash route, normalised to a clean path
      page_title: document.title,  // docsify sets <title> per page
      page_location: location.origin + '/#' + path
    });
  });
}
```

Why this shape:

- **`doneEach` owns every page view; the container's page-load hit owns none.**
  The GA4 configuration tag in GTM is set with its automatic page-view **off**,
  and a GA4 event tag fires `page_view` only on `spa_page_view`. One producer of
  page views means no way to double-count and no way to miss the first render —
  `doneEach` fires on the initial render too. Letting the container send the
  first and the plugin send the rest would have to special-case the entry page
  and is one refactor away from either a duplicate or a hole.
- **`page_path` is the hash route, normalised.** `vm.route.path` is docsify's
  own idea of the current page (`/webflux`, `/rfc/0019-docs-google-analytics`),
  which is exactly the granularity a reader thinks in — not the raw
  `location.pathname`, which is always `/` on a hash SPA and would collapse every
  page into one row.
- **It fails closed.** `dataLayer.push` is a plain array append, always defined
  by the container snippet (and re-guarded `window.dataLayer || []` in case the
  push runs before gtm.js). If GTM is blocked (ad-blocker, DNT extension, a
  network hiccup) the entries simply accumulate unread — the push never throws
  and the docs render exactly as today. Analytics is never allowed to break a
  page.

## Configuration

- **The GTM container ID (`GTM-W48ZV6XD`) is public by design** — it ships in
  client-side JS on every GTM site and identifies a *container*, not a secret.
  It is embedded in the `index.html` snippet. (No Measurement Protocol API secret
  is used here; everything flows through the browser container.)
- **GTM is the indirection on purpose.** The GA4 measurement ID lives **inside
  the container**, not in the repo — so the tag config (which GA4 property, IP
  anonymisation, Consent Mode, any future custom event) can change in the GTM
  console without a docs deploy. The repo commits to one thing: pushing a
  correctly-timed `spa_page_view` per navigation. The GA4 page-view tag, its
  trigger on `spa_page_view`, and turning **off** the config tag's automatic
  page-view are configured in GTM before this goes live.
- **No build wiring.** gtm.js loads from `googletagmanager.com` at runtime, the
  same pattern as docsify, mermaid and prism already use from jsDelivr — GitHub
  Pages serves a static file, there is nothing to configure server-side.

## Non-goals

- **No engine, `core/` or `reactive/` change.** This is a docs-site file. The
  library ships zero analytics and zero new dependency.
- **No custom event taxonomy** (search terms, copy-code clicks, outbound link
  tracking, scroll depth). This RFC delivers the one requirement — page views per
  navigation. Custom events can be a follow-up; because everything routes through
  GTM, most can be added as tags/triggers in the console with no code change, or
  as another `dataLayer.push` when the DOM signal is needed.
- **No consent banner / CMP in this RFC.** See Risks — this is a deliberate
  scoping decision to call out, not silently skip, because it has legal weight
  depending on audience and region.
- **No server-side or Measurement-Protocol tracking.** Browser gtag.js only.
- **No second analytics vendor**, no self-hosted alternative (Plausible/Umami)
  evaluated here — the request is specifically Google Analytics.

## Testing

The docs have no unit suite; the guardrails are manual and mechanical:

1. **The page-view fires per navigation, not per load.** Open the site in GTM
   **Preview mode** (and/or GA4 DebugView): confirm one `spa_page_view` on the
   landing render and **one more on each** move to Quickstart → WebFlux → an RFC
   → back, and that the GA4 tag turns each into a `page_view` whose `page_path`
   matches `vm.route.path`.
2. **No double count on entry.** The landing page produces exactly one
   `page_view`, not two — the check that the GA4 config tag's automatic page-view
   is OFF and only the `spa_page_view` trigger fires it.
3. **Blocked-GTM degradation.** Load with gtm.js blocked (uBlock, or DevTools
   request-block on `googletagmanager.com`); the site renders and navigates
   normally, no console error, no thrown exception from the plugin (the
   `dataLayer.push` just appends to an unread array).
4. **The existing `doneEach` consumers still run** — navbar restore, mermaid,
   cover copy — i.e. adding a fourth plugin did not disturb the three already in
   `index.html`.

Because the change is a hash-route SPA concern, the load-bearing case is #1: a
reviewer must see the *second* and *third* page views appear without a reload,
which is the exact behaviour the naive install fails to produce.

## Risks

- **Privacy / consent is real and out of this RFC's mechanism.** GA4 sets
  cookies and processes IP-derived data; depending on the audience and
  jurisdiction (GDPR/ePrivacy) a public site may owe a consent gate before
  loading gtag.js. This RFC ships the *tracking*, not the *consent policy* — the
  decision to add a CMP, enable GA's Consent Mode, or rely on IP anonymisation is
  called out here so it is made on purpose. Routing through GTM helps: a consent
  gate or **GTM Consent Mode** can be added in the console without touching the
  repo, and `doneEach` still owns the page views once granted. **Whether consent
  is required for nioflow.dev's audience remains an operational decision to close
  in the GTM console, independent of this code.**
- **A hard third-party dependency on `googletagmanager.com`.** gtm.js loads
  `async` and the plugin only ever appends to an array, so a blocked or slow
  container never delays or breaks a render — but it is one more external origin
  the site talks to, and readers who block it simply go uncounted (acceptable;
  better than a page that breaks). GTM also loads GA4 (and anything else added to
  the container later) client-side, so the *set* of third-party origins can grow
  in the console without a repo change — a convenience worth naming.
- **Route/path drift.** If docsify's routing config changes (e.g. History-API
  mode instead of hash), `vm.route.path` stays correct but `page_location` should
  be revisited. The path is read from docsify's own view-model precisely so it
  tracks the router rather than guessing from the URL.
- **Under-counting vs the naive install, by design.** DNT users, ad-blockers and
  no-JS readers are invisible. That is the correct trade for a docs site; we
  optimise for not-breaking over completeness.
