// Render nomnoml diagrams into SVG, surviving HTMX swaps.
//
// A diagram is a `.diagram` element containing a hidden
// `pre.nomnoml-source` with nomnoml text. The rendered SVG goes into a
// `.nomnoml-out` sibling created on demand. HTMX swaps replace the whole
// content panel every poll, so re-render after every swap.

function renderDiagrams() {
  if (typeof nomnoml === "undefined") return;
  document.querySelectorAll(".diagram").forEach(function (container) {
    var source = container.querySelector(".nomnoml-source");
    if (!source) return;
    var out = container.querySelector(".nomnoml-out");
    if (!out) {
      out = document.createElement("div");
      out.className = "nomnoml-out";
      container.appendChild(out);
    }
    try {
      out.innerHTML = nomnoml.renderSvg(source.textContent);
    } catch (err) {
      out.textContent = "diagram error: " + err.message;
    }
  });
}

document.addEventListener("DOMContentLoaded", renderDiagrams);
document.addEventListener("htmx:afterSwap", renderDiagrams);
