// search script, borrowed from book theme

function debounce(func, wait) {
    var timeout;
  
    return function () {
      var context = this;
      var args = arguments;
      clearTimeout(timeout);
  
      timeout = setTimeout(function () {
        timeout = null;
        func.apply(context, args);
      }, wait);
    };
  }
  
  // Taken from mdbook
  // The strategy is as follows:
  // First, assign a value to each word in the document:
  //  Words that correspond to search terms (stemmer aware): 40
  //  Normal words: 2
  //  First word in a sentence: 8
  // Then use a sliding window with a constant number of words and count the
  // sum of the values of the words within the window. Then use the window that got the
  // maximum sum. If there are multiple maximas, then get the last one.
  // Enclose the terms in <b>.
  function makeTeaser(body, terms) {
    var TERM_WEIGHT = 40;
    var NORMAL_WORD_WEIGHT = 2;
    var FIRST_WORD_WEIGHT = 8;
    var TEASER_MAX_WORDS = 30;
  
    var stemmedTerms = terms.map(function (w) {
      return elasticlunr.stemmer(w.toLowerCase());
    });
    var termFound = false;
    var index = 0;
    var weighted = []; // contains elements of ["word", weight, index_in_document]
  
    // split in sentences, then words
    var sentences = body.toLowerCase().split(". ");
  
    for (var i in sentences) {
      var words = sentences[i].split(" ");
      var value = FIRST_WORD_WEIGHT;
  
      for (var j in words) {
        var word = words[j];
  
        if (word.length > 0) {
          for (var k in stemmedTerms) {
            if (elasticlunr.stemmer(word).startsWith(stemmedTerms[k])) {
              value = TERM_WEIGHT;
              termFound = true;
            }
          }
          weighted.push([word, value, index]);
          value = NORMAL_WORD_WEIGHT;
        }
  
        index += word.length;
        index += 1;  // ' ' or '.' if last word in sentence
      }
  
      index += 1;  // because we split at a two-char boundary '. '
    }
  
    if (weighted.length === 0) {
      return body;
    }
  
    var windowWeights = [];
    var windowSize = Math.min(weighted.length, TEASER_MAX_WORDS);
    // We add a window with all the weights first
    var curSum = 0;
    for (var i = 0; i < windowSize; i++) {
      curSum += weighted[i][1];
    }
    windowWeights.push(curSum);
  
    for (var i = 0; i < weighted.length - windowSize; i++) {
      curSum -= weighted[i][1];
      curSum += weighted[i + windowSize][1];
      windowWeights.push(curSum);
    }
  
    // If we didn't find the term, just pick the first window
    var maxSumIndex = 0;
    if (termFound) {
      var maxFound = 0;
      // backwards
      for (var i = windowWeights.length - 1; i >= 0; i--) {
        if (windowWeights[i] > maxFound) {
          maxFound = windowWeights[i];
          maxSumIndex = i;
        }
      }
    }
  
    var teaser = [];
    var startIndex = weighted[maxSumIndex][2];
    for (var i = maxSumIndex; i < maxSumIndex + windowSize; i++) {
      var word = weighted[i];
      if (startIndex < word[2]) {
        // missing text from index to start of `word`
        teaser.push(body.substring(startIndex, word[2]));
        startIndex = word[2];
      }
  
      // add <em/> around search terms
      if (word[1] === TERM_WEIGHT) {
        teaser.push("<b>");
      }
      startIndex = word[2] + word[0].length;
      teaser.push(body.substring(word[2], startIndex));
  
      if (word[1] === TERM_WEIGHT) {
        teaser.push("</b>");
      }
    }
    teaser.push("…");
    return teaser.join("");
  }
  
  function formatSearchResultItem(item, terms) {
    var li = document.createElement("li");
    li.classList.add("search-results__item");
    li.innerHTML = `<a href="${item.ref}">${item.doc.title}</a>`;
    li.innerHTML += `<div class="search-results__teaser">${makeTeaser(item.doc.body, terms)}</div>`;
    return li;
  }
  
  // Go from the book view to the search view
  function toggleSearchMode() {
    var $wrapContent = document.querySelector("#wrap");
    var $searchIcon = document.querySelector("#search-ico");
    var $searchContainer = document.querySelector(".search-container");
    if ($searchContainer.classList.contains("search-container--is-visible")) {
      $searchContainer.classList.remove("search-container--is-visible");
      $wrapContent.style.display = "";
      $searchIcon.className = "ms-Icon--Search";
    } else {
      $searchContainer.classList.add("search-container--is-visible");
      $wrapContent.style.display = "none";
      $searchIcon.className = "ms-Icon--ChromeClose";
      document.getElementById("search").focus();
    }
  }
  
  function initSearch() {
    var $searchInput = document.getElementById("search");
    if (!$searchInput) {
      return;
    }
    var $searchIcon = document.querySelector("#search-ico");
    $searchIcon.addEventListener("click", toggleSearchMode);
  
    var $searchResults = document.querySelector(".search-results");
    var $searchResultsHeader = document.querySelector(".search-results__header");
    var $searchResultsItems = document.querySelector(".search-results__items");
    var MAX_ITEMS = 100;
  
    var options = {
      bool: "AND",
      fields: {
        title: {boost: 2},
        body: {boost: 1},
      }
    };
    var currentTerm = "";
    var index = elasticlunr.Index.load(window.searchIndex);
  
    $searchInput.addEventListener("keyup", debounce(function() {
      var term = $searchInput.value.trim();
      if (term === currentTerm || !index) {
        return;
      }
      $searchResults.style.display = term === "" ? "none" : "block";
      $searchResultsItems.innerHTML = "";
      if (term === "") {
        return;
      }
  
      var results = index.search(term, options).filter(function (r) {
        return r.doc.body !== "";
      });
      if (results.length === 0) {
        $searchResultsHeader.innerText = `Nothing like «${term}»`;
        return;
      }
  
      currentTerm = term;
        $searchResultsHeader.innerText = `${results.length} found for «${term}»:`;
      for (var i = 0; i < Math.min(results.length, MAX_ITEMS); i++) {
        if (!results[i].doc.body) {
          continue;
        }
        // var item = document.createElement("li");
        // item.innerHTML = formatSearchResultItem(results[i], term.split(" "));
        console.log(results[i]);
        $searchResultsItems.appendChild(formatSearchResultItem(results[i], term.split(" ")));
      }
    }, 150));
  }
  
  if (document.readyState === "complete" ||
      (document.readyState !== "loading" && !document.documentElement.doScroll)
  ) {
    initSearch();
  } else {
    document.addEventListener("DOMContentLoaded", initSearch);
  }

// mobile 

  function burger() {
    let x = document.querySelector("#trees");
    let y = document.querySelector("#mobile");

    if (x.style.display === "block") {
      x.style.display = "none";
      y.className = "ms-Icon--GlobalNavButton";
    } else {
      x.style.display = "block";
      y.className = "ms-Icon--ChromeClose";
    }
  }

// https://aaronluna.dev/blog/add-copy-button-to-code-blocks-hugo-chroma/

function createCopyButton(highlightDiv) {
  const button = document.createElement("button");
  button.className = "copy-code-button ";
  button.type = "button";
  button.innerHTML = "&#xE8C8;";
  button.addEventListener("click", () =>
    copyCodeToClipboard(button, highlightDiv)
  );
  addCopyButtonToDom(button, highlightDiv);
}

async function copyCodeToClipboard(button, highlightDiv) {
  const codeToCopy = highlightDiv.querySelector(":last-child > code")
    .innerText;
  try {
    result = await navigator.permissions.query({ name: "clipboard-write" });
    if (result.state == "granted" || result.state == "prompt") {
      await navigator.clipboard.writeText(codeToCopy);
    } else {
      copyCodeBlockExecCommand(codeToCopy, highlightDiv);
    }
  } catch (_) {
    copyCodeBlockExecCommand(codeToCopy, highlightDiv);
  } finally {
    codeWasCopied(button);
  }
}

function copyCodeBlockExecCommand(codeToCopy, highlightDiv) {
  const textArea = document.createElement("textArea");
  textArea.contentEditable = "true";
  textArea.readOnly = "false";
  textArea.className = "copyable-text-area";
  textArea.value = codeToCopy;
  highlightDiv.insertBefore(textArea, highlightDiv.firstChild);
  const range = document.createRange();
  range.selectNodeContents(textArea);
  const sel = window.getSelection();
  sel.removeAllRanges();
  sel.addRange(range);
  textArea.setSelectionRange(0, 999999);
  document.execCommand("copy");
  highlightDiv.removeChild(textArea);
}

function codeWasCopied(button) {
  button.blur();
  button.innerHTML = "&#xE74E;";
  setTimeout(function () {
    button.innerHTML = "&#xE8C8;";
  }, 2000);
}

function addCopyButtonToDom(button, highlightDiv) {
  highlightDiv.insertBefore(button, highlightDiv.firstChild);
  const wrapper = document.createElement("div");
  wrapper.className = "highlight-wrapper";
  highlightDiv.parentNode.insertBefore(wrapper, highlightDiv);
  wrapper.appendChild(highlightDiv);
}

document
  .querySelectorAll("pre")
  .forEach((highlightDiv) => createCopyButton(highlightDiv));
