+++
title = "Overview"
weight = 5
+++

## Zola at a Glance

Zola is a static site generator (SSG), similar to [Hugo](https://gohugo.io/), [Pelican](https://blog.getpelican.com/), and [Jekyll](https://jekyllrb.com/) (for a comprehensive list of SSGs, please see the [StaticGen](https://www.staticgen.com/) site). It is written in [Rust](https://www.rust-lang.org/) and uses the [Tera](https://tera.netlify.com/) template engine, which is similar to [Jinja2](https://jinja.palletsprojects.com/en/2.10.x/), [Django templates](https://docs.djangoproject.com/en/2.2/topics/templates/), [Liquid](https://shopify.github.io/liquid/), and [Twig](https://twig.symfony.com/). Content is written in [CommonMark](https://commonmark.org/), a strongly defined, highly compatible specification of [Markdown](https://www.markdownguide.org/).

SSGs use dynamic templates to transform content into static HTML pages. Static sites are thus very fast and require no databases, making them easy to host. A comparison between static and dynamic sites, such as WordPress, Drupal, and Django, can be found [here](https://dev.to/ashenmaster/static-vs-dynamic-sites-61f).

To get a taste of Zola, please see the quick overview below.

## First Steps with Zola

Unlike some SSGs, Zola makes no assumptions regarding the structure of your site. In this overview, we'll be making a simple blog site.

### Initialize Site

> This overview is based on Zola 0.9.

Please see the detailed [installation instructions for your platform](@/getting-started/installation.md). With Zola installed, let's initialize our site:

```bash
$ zola init myblog
```

You will be asked a few questions.

```
> What is the URL of your site? (https://example.com):
> Do you want to enable Sass compilation? [Y/n]:
> Do you want to enable syntax highlighting? [y/N]:
> Do you want to build a search index of the content? [y/N]:
```

 For our blog, let's accept the default values (i.e., press Enter for each question). We now have a `myblog` directory with the following structure:

```bash
├── config.toml
├── content
├── sass
├── static
├── templates
└── themes
```

Let's start the Zola development server with:

```bash
$ zola serve
Building site...
-> Creating 0 pages (0 orphan), 0 sections, and processing 0 images
```

> This command must be run in the base Zola directory, which contains `config.toml`.

If you point your web browser to <http://127.0.0.1:1111>, you should see a "Welcome to Zola" message.

### Home Page

Let's make a home page. To do this, let's first create a `base.html` file inside the `templates` directory. This step will make more sense as we move through this overview. We'll be using the CSS framework [Bulma](https://bulma.io/).

```html
<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <title>MyBlog</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.8.0/css/bulma.min.css">
</head>

<body>
  <section class="section">
    <div class="container">
      {% block content %} {% endblock %}
    </div>
  </section>
</body>

</html>
```  

Now, let's create an `index.html` file inside the `templates` directory.

```html
{% extends "base.html" %}

{% block content %}
<h1 class="title">
  This is my blog made with Zola.
</h1>
{% endblock content %}
```  

This tells Zola that `index.html` extends our `base.html` file and replaces the block called "content" with the text between the `{% block content %}` and `{% endblock content %}` tags.

### Content Directory

Now let's add some content. We'll start by making a `blog` subdirectory in the `content` directory and creating an `_index.md` file inside it. This file tells Zola that `blog` is a [section](@/content/section.md), which is how content is categorized in Zola.

```bash
├── content
│   └── blog
│       └── _index.md
```

In the `_index.md` file, we'll set the following variables in [TOML](https://github.com/toml-lang/toml) format:

```md
+++
title = "List of blog posts"
sort_by = "date"
template = "blog.html"
page_template = "blog-page.html"
+++
```

> Note that although no variables are mandatory, the opening and closing `+++` are required.

* *sort_by = "date"* tells Zola to use the date to order our section pages (more on pages below). 
* *template = "blog.html"* tells Zola to use `blog.html` in the `templates` directory as the template for listing the Markdown files in this section. 
* *page_template = "blog-page.html"* tells Zola to use `blog-page.html` in the `templates` directory as the template for individual Markdown files. 

For a full list of section variables, please see the [section](@/content/section.md) documentation. We will use *title = "List of blog posts"* in a template (see below).

### Templates

Let's now create some more templates. In the `templates` directory, create a `blog.html` file with the following contents:

```html
{% extends "base.html" %}

{% block content %}
<h1 class="title">
  {{ section.title }}
</h1>
<ul>
  {% for page in section.pages %}
  <li><a href="{{ page.permalink | safe }}">{{ page.title }}</a></li>
  {% endfor %}
</ul>
{% endblock content %}
```

As done by `index.html`, `blog.html` extends `base.html`, but this time we want to list the blog posts. The *title* we set in the `_index.md` file above is available to us as `{{ section.title }}`. In the list below the title, we loop through all the pages in our section (`blog` directory) and output the page title and URL using `{{ page.title }}` and `{{ page.permalink | safe }}`, respectively. We use the `| safe` filter because the permalink doesn't need to be HTML escaped (escaping would cause `/` to render as `&#x2F;`).

If you go to <http://127.0.0.1:1111/blog/>, you will see the section page for `blog`. The list is empty because we don't have any blog posts. Let's fix that now.

### Markdown Content

In the `blog` directory, create a file called `first.md` with the following contents:

```md
+++
title = "My first post"
date = 2019-11-27
+++

This is my first blog post.
```

The *title* and *date* will be available to us in the `blog-page.html` template as `{{ page.title }}` and `{{ page.date }}`, respectively. All text below the closing `+++` will be available to us as `{{ page.content }}`.

We now need to make the `blog-page.html` template. In the `templates` directory, create this file with the contents:

```html
{% extends "base.html" %}

{% block content %}
<h1 class="title">
  {{ page.title }}
</h1>
<p class="subtitle"><strong>{{ page.date }}</strong></p>
{{ page.content | safe }}
{% endblock content %}
```

> Note the `| safe` filter for `{{ page.content }}`.

This should start to look familiar. If you now go back to our blog list page at <http://127.0.0.1:1111/blog/>, you should see our lonely post. Let's add another. In the `content/blog` directory, let's create the file `second.md` with the contents:

```md
+++
title = "My second post"
date = 2019-11-28
+++

This is my second blog post.
```

Back at <http://127.0.0.1:1111/blog/>, our second post shows up on top of the list because it's newer than the first post and we had set *sort_by = "date"* in our `_index.md` file. As a final step, let's modify our home page to link to our blog posts.

The `index.html` file inside the `templates` directory should be:

```html
{% extends "base.html" %}

{% block content %}
<h1 class="title">
  This is my blog made with Zola.
</h1>
<p>Click <a href="/blog/">here</a> to see my posts.</p>
{% endblock content %}
```  

This has been a quick overview of Zola. You can now dive into the rest of the documentation.
