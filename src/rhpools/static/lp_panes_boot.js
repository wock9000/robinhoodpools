(() => {
  "use strict";
  try {
    const saved = JSON.parse(localStorage.getItem("lp-terminal-pane-layout-v1") || "{}");
    const mobile = matchMedia("(max-width: 520px), (max-height: 520px) and (max-width: 900px)").matches;
    const sizes = saved[mobile ? "mobile" : "desktop"];
    if (!Array.isArray(sizes) || sizes.length !== 3 || !sizes.every(Number.isFinite)) return;
    if (!sizes.every(size => mobile ? size >= 25 && size <= 160 : size >= .05 && size <= .9)) return;
    const unit = mobile ? "dvh" : "fr";
    ["--pane-tape-size", "--pane-owners-size", "--pane-pools-size"].forEach((property, index) => {
      document.documentElement.style.setProperty(property, `${sizes[index]}${unit}`);
    });
  } catch (_) {
    // Unavailable storage or obsolete preferences leave stable CSS defaults.
  }
})();
