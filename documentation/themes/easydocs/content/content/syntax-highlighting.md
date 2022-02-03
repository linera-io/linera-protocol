+++
title = "Syntax Highlighting"
weight = 80
+++

Zola comes with built-in syntax highlighting but you first
need to enable it in the configuration

Once this is done, Zola will automatically highlight all code blocks
in your content. A code block in Markdown looks like the following:

````md

```rust
let highlight = true;
```

````

You can replace `rust` with another language or not put anything to get the text
interpreted as plain text.

Here is a full list of supported languages and their short names:

```
- ActionScript -> ["as"]
- Advanced CSV -> ["csv", "tsv"]
- AppleScript -> ["applescript", "script editor"]
- ASP -> ["asa"]
- Assembly x86 (NASM) -> ["asm", "inc", "nasm"]
- AWK -> ["awk"]
- Batch File -> ["bat", "cmd"]
- BibTeX -> ["bib"]
- Bourne Again Shell (bash) -> [".bash_aliases", ".bash_completions", ".bash_functions", ".bash_login", ".bash_logout", ".bash_profile", ".bash_variables", ".bashrc", ".ebuild", ".eclass", ".profile", ".textmate_init", ".zlogin", ".zlogout", ".zprofile", ".zshenv", ".zshrc", "PKGBUILD", "ash", "bash", "sh", "zsh"]
- C -> ["c", "h"]
- C# -> ["cs", "csx"]
- C++ -> ["C", "c++", "cc", "cp", "cpp", "cxx", "h", "h++", "hh", "hpp", "hxx", "inl", "ipp"]
- ClojureC -> ["boot", "clj", "cljc", "cljs", "cljx"]
- CMake -> ["CMakeLists.txt", "cmake"]
- CMake C Header -> ["h.in"]
- CMake C++ Header -> ["h++.in", "hh.in", "hpp.in", "hxx.in"]
- CMakeCache -> ["CMakeCache.txt"]
- Crystal -> ["cr"]
- CSS -> ["css", "css.erb", "css.liquid"]
- D -> ["d", "di"]
- Dart -> ["dart"]
- Diff -> ["diff", "patch"]
- Dockerfile -> ["Dockerfile", "dockerfile"]
- EDN -> ["edn"]
- Elixir -> ["ex", "exs"]
- Elm -> ["elm"]
- Erlang -> ["Emakefile", "emakefile", "erl", "escript", "hrl"]
- F# -> ["fs", "fsi", "fsx"]
- Fortran (Fixed Form) -> ["F", "F77", "FOR", "FPP", "f", "f77", "for", "fpp"]
- Fortran (Modern) -> ["F03", "F08", "F90", "F95", "f03", "f08", "f90", "f95"]
- Fortran Namelist -> ["namelist"]
- Friendly Interactive Shell (fish) -> ["fish"]
- GDScript (Godot Engine) -> ["gd"]
- Generic Config -> [".dircolors", ".gitattributes", ".gitignore", ".gitmodules", ".inputrc", "Doxyfile", "cfg", "conf", "config", "dircolors", "gitattributes", "gitignore", "gitmodules", "ini", "inputrc", "mak", "mk", "pro"]
- Git Attributes -> [".gitattributes", "attributes", "gitattributes"]
- Git Commit -> ["COMMIT_EDITMSG", "MERGE_MSG", "TAG_EDITMSG"]
- Git Config -> [".gitconfig", ".gitmodules", "gitconfig"]
- Git Ignore -> [".gitignore", "exclude", "gitignore"]
- Git Link -> [".git"]
- Git Log -> ["gitlog"]
- Git Mailmap -> [".mailmap", "mailmap"]
- Git Rebase Todo -> ["git-rebase-todo"]
- GLSL -> ["comp", "frag", "fs", "fsh", "fshader", "geom", "glsl", "gs", "gsh", "gshader", "tesc", "tese", "vert", "vs", "vsh", "vshader"]
- Go -> ["go"]
- GraphQL -> ["gql", "graphql", "graphqls"]
- Graphviz (DOT) -> ["DOT", "dot", "gv"]
- Groovy -> ["Jenkinsfile", "gradle", "groovy", "gvy"]
- Handlebars -> ["handlebars", "handlebars.html", "hbr", "hbrs", "hbs", "hdbs", "hjs", "mu", "mustache", "rac", "stache", "template", "tmpl"]
- Haskell -> ["hs"]
- HTML -> ["htm", "html", "shtml", "xhtml"]
- HTML (ASP) -> ["asp"]
- HTML (EEx) -> ["html.eex", "html.leex"]
- HTML (Erlang) -> ["yaws"]
- HTML (Jinja2) -> ["htm.j2", "html.j2", "xhtml.j2", "xml.j2"]
- HTML (Rails) -> ["erb", "html.erb", "rails", "rhtml"]
- HTML (Tcl) -> ["adp"]
- Java -> ["bsh", "java"]
- Java Properties -> ["properties"]
- Java Server Page (JSP) -> ["jsp"]
- JavaScript -> ["htc", "js"]
- JavaScript (Rails) -> ["js.erb"]
- Jinja2 -> ["j2", "jinja", "jinja2"]
- JSON -> ["Pipfile.lock", "ipynb", "json", "sublime-build", "sublime-color-scheme", "sublime-commands", "sublime-completions", "sublime-keymap", "sublime-macro", "sublime-menu", "sublime-mousemap", "sublime-project", "sublime-settings", "sublime-theme"]
- Julia -> ["jl"]
- Kotlin -> ["kt", "kts"]
- LaTeX -> ["ltx", "tex"]
- Less -> ["css.less", "less"]
- Linker Script -> ["ld"]
- Lisp -> ["cl", "clisp", "el", "fasl", "l", "lisp", "lsp", "mud", "scm", "ss"]
- Literate Haskell -> ["lhs"]
- lrc -> ["lrc", "lyric"]
- Lua -> ["lua"]
- Makefile -> ["GNUmakefile", "Makefile", "Makefile.am", "Makefile.in", "OCamlMakefile", "mak", "make", "makefile", "makefile.am", "makefile.in", "mk"]
- Markdown -> ["markdn", "markdown", "md", "mdown"]
- MATLAB -> ["matlab"]
- MiniZinc (MZN) -> ["dzn", "mzn"]
- NAnt Build File -> ["build"]
- Nim -> ["nim", "nims"]
- Nix -> ["nix"]
- Objective-C -> ["h", "m"]
- Objective-C++ -> ["M", "h", "mm"]
- OCaml -> ["ml", "mli"]
- OCamllex -> ["mll"]
- OCamlyacc -> ["mly"]
- Pascal -> ["dpr", "p", "pas"]
- Perl -> ["pc", "pl", "pm", "pmc", "pod", "t"]
- PHP -> ["php", "php3", "php4", "php5", "php7", "phps", "phpt", "phtml"]
- Plain Text -> ["txt"]
- PowerShell -> ["ps1", "psd1", "psm1"]
- PureScript -> ["purs"]
- Python -> ["SConscript", "SConstruct", "Sconstruct", "Snakefile", "bazel", "bzl", "cpy", "gyp", "gypi", "pxd", "pxd.in", "pxi", "pxi.in", "py", "py3", "pyi", "pyw", "pyx", "pyx.in", "rpy", "sconstruct", "vpy", "wscript"]
- R -> ["R", "Rprofile", "r"]
- Racket -> ["rkt"]
- Rd (R Documentation) -> ["rd"]
- Reason -> ["re", "rei"]
- Regular Expression -> ["re"]
- Regular Expressions (Elixir) -> ["ex.re"]
- reStructuredText -> ["rest", "rst"]
- Ruby -> ["Appfile", "Appraisals", "Berksfile", "Brewfile", "Cheffile", "Deliverfile", "Fastfile", "Gemfile", "Guardfile", "Podfile", "Rakefile", "Rantfile", "Scanfile", "Snapfile", "Thorfile", "Vagrantfile", "capfile", "cgi", "config.ru", "fcgi", "gemspec", "irbrc", "jbuilder", "podspec", "prawn", "rabl", "rake", "rb", "rbx", "rjs", "ruby.rail", "simplecov", "thor"]
- Ruby Haml -> ["haml", "sass"]
- Ruby on Rails -> ["builder", "rxml"]
- Rust -> ["rs"]
- Sass -> ["sass"]
- Scala -> ["sbt", "sc", "scala"]
- SCSS -> ["scss"]
- SQL -> ["ddl", "dml", "sql"]
- SQL (Rails) -> ["erbsql", "sql.erb"]
- srt -> ["srt", "subrip"]
- Stylus -> ["styl", "stylus"]
- SWI-Prolog -> ["pro"]
- Swift -> ["swift"]
- Tcl -> ["tcl"]
- TeX -> ["cls", "sty"]
- Textile -> ["textile"]
- TOML -> ["Cargo.lock", "Gopkg.lock", "Pipfile", "tml", "toml"]
- TypeScript -> ["ts"]
- TypeScriptReact -> ["tsx"]
- VimL -> ["vim"]
- XML -> ["dtml", "opml", "rng", "rss", "svg", "tld", "xml", "xsd", "xslt"]
- YAML -> ["sublime-syntax", "yaml", "yml"]
```

Note: due to some issues with the JavaScript syntax, the TypeScript syntax will be used instead.
If 

If you want to highlight a language not on this list, please open an issue or a pull request on the [Zola repo](https://github.com/getzola/zola).
Alternatively, the `extra_syntaxes` configuration option can be used to add additional syntax files.

If your site source is laid out as follows:

```
.
├── config.toml
├── content/
│   └── ...
├── static/
│   └── ...
├── syntaxes/
│   ├── Sublime-Language1/
│   │   └── lang1.sublime-syntax
│   └── lang2.sublime-syntax
└── templates/
    └── ...
```

you would set your `extra_syntaxes` to `["syntaxes", "syntaxes/Sublime-Language1"]` to load `lang1.sublime-syntax` and `lang2.sublime-syntax`.
