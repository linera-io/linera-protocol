+++
title = "CLI usage"
weight = 15
+++

Zola only has 4 commands: `init`, `build`, `serve` and `check`.

You can view the help for the whole program by running `zola --help` and
that for a specific command by running `zola <cmd> --help`.

## init

Creates the directory structure used by Zola at the given directory after asking a few basic configuration questions.
Any choices made during these prompts can be easily changed by modifying `config.toml`.

```bash
$ zola init my_site
$ zola init
```

If the `my_site` directory already exists, Zola will only populate it if it contains only hidden files (dotfiles are ignored). If no `my_site` argument is passed, Zola will try to populate the current directory.

In case you want to attempt to populate a non-empty directory and are brave, you can use `zola init --force`. Note that this will _not_ overwrite existing folders or files; in those cases you will get a `File exists (os error 17)` error or similar.

You can initialize a git repository and a Zola site directly from within a new folder:

```bash
$ git init
$ zola init
```

## build

This will build the whole site in the `public` directory (if this directory already exists, it is overwritten).

```bash
$ zola build
```

You can override the config `base_url` by passing a new URL to the `base-url` flag.

```bash
$ zola build --base-url $DEPLOY_URL
```

This is useful for example when you want to deploy previews of a site to a dynamic URL, such as Netlify
deploy previews.

You can override the default output directory `public` by passing another value to the `output-dir` flag.

```bash
$ zola build --output-dir $DOCUMENT_ROOT
```

You can point to a config file other than `config.toml` like so (note that the position of the `config` option is important):

```bash
$ zola --config config.staging.toml build
```

You can also process a project from a different directory with the `root` flag. If building a project 'out-of-tree' with the `root` flag, you may want to combine it with the `output-dir` flag. (Note that like `config`, the position is important):
```bash
$ zola --root /path/to/project build
```

By default, drafts are not loaded. If you wish to include them, pass the `--drafts` flag.

## serve

This will build and serve the site using a local server. You can also specify
the interface/port combination to use if you want something different than the default (`127.0.0.1:1111`).

You can also specify different addresses for the interface and base_url using `--interface` and `-u`/`--base-url`, respectively, if for example you are running Zola in a Docker container.

Use the `--open` flag to automatically open the locally hosted instance in your
web browser.

In the event you don't want Zola to run a local web server, you can use the `--watch-only` flag.

Before starting, Zola will delete the `public` directory to start from a clean slate.

```bash
$ zola serve
$ zola serve --port 2000
$ zola serve --interface 0.0.0.0
$ zola serve --interface 0.0.0.0 --port 2000
$ zola serve --interface 0.0.0.0 --base-url 127.0.0.1
$ zola serve --interface 0.0.0.0 --port 2000 --output-dir www/public
$ zola serve --watch-only
$ zola serve --open
```

The serve command will watch all your content and provide live reload without
a hard refresh if possible.

Some changes cannot be handled automatically and thus live reload may not always work. If you
fail to see your change or get an error, try restarting `zola serve`.


You can also point to a config file other than `config.toml` like so (note that the position of the `config` option is important):

```bash
$ zola --config config.staging.toml serve
```

By default, drafts are not loaded. If you wish to include them, pass the `--drafts` flag.

## check

The check subcommand will try to build all pages just like the build command would, but without writing any of the
results to disk. Additionally, it will also check all external links in Markdown files by trying to fetch
them (links in the template files are not checked).

By default, drafts are not loaded. If you wish to include them, pass the `--drafts` flag.

## Colored output

Colored output is used if your terminal supports it.

*Note*: coloring is automatically disabled when the output is redirected to a pipe or a file (i.e., when the standard output is not a TTY).

You can disable this behavior by exporting one of the following two environment variables:

- `NO_COLOR` (the value does not matter)
- `CLICOLOR=0`

To force the use of colors, you can set the following environment variable:

- `CLICOLOR_FORCE=1`
