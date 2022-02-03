+++
title = "Directory structure"
weight = 30
+++

After running `zola init`, you should see the following structure in your directory:


```bash
.
├── config.toml
├── content
├── sass
├── static
├── templates
└── themes

5 directories, 1 file
```

Here's a high-level overview of each of these directories and `config.toml`.

## `config.toml`
A mandatory Zola configuration file in TOML format.
This file is explained in detail in the [configuration documentation](@/getting-started/configuration.md).

## `content`
Contains all your markup content (mostly `.md` files).
Each child directory of the `content` directory represents a [section](@/content/section.md)
that contains [pages](@/content/page.md) (your `.md` files).

To learn more, read the [content overview page](@/content/overview.md).

## `sass`
Contains the [Sass](http://sass-lang.com) files to be compiled. Non-Sass files will be ignored.
The directory structure of the `sass` folder will be preserved when copying over the compiled files; for example, a file at
`sass/something/site.scss` will be compiled to `public/something/site.css`.

## `static`
Contains any kind of file. All the files/directories in the `static` directory will be copied as-is to the output directory.
If your static files are large, you can configure Zola to [hard link](https://en.wikipedia.org/wiki/Hard_link) them
instead of copying them by setting `hard_link_static = true` in the config file.

## `templates`
Contains all the [Tera](https://tera.netlify.com) templates that will be used to render your site.
Have a look at the [templates documentation](@/templates/_index.md) to learn more about default templates
and available variables.

## `themes`
Contains themes that can be used for your site. If you are not planning to use themes, leave this directory empty.
If you want to learn about themes, see the [themes documentation](@/themes/_index.md).
