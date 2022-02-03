+++
title = "Robots.txt"
weight = 70
+++

Zola will look for a `robots.txt` file in the `templates` directory or
use the built-in one.

Robots.txt is the simplest of all templates: it only gets `config`
and the default is what most sites want:

```jinja2
User-agent: *
Allow: /
Sitemap: {{/* get_url(path="sitemap.xml") */}}
```
