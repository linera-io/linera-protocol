+++
title = "Taxonomies"
weight = 40
+++

Zola will look up the following files in the `templates` directory:

- `$TAXONOMY_NAME/single.html`
- `$TAXONOMY_NAME/list.html`

First, `TaxonomyTerm` has the following fields:

```ts
name: String;
slug: String;
permalink: String;
pages: Array<Page>;
```

and `TaxonomyConfig` has the following fields:

```ts
name: String,
paginate_by: Number?;
paginate_path: String?;
feed: Bool;
lang: String;
```


### Taxonomy list (`list.html`)

This template is never paginated and therefore gets the following variables in all cases.

```ts
// The site config
config: Config;
// The data of the taxonomy, from the config
taxonomy: TaxonomyConfig;
// The current full permalink for that page
current_url: String;
// The current path for that page
current_path: String;
// All terms for that taxonomy
terms: Array<TaxonomyTerm>;
// The lang of the current page
lang: String;
```


### Single term (`single.html`)
```ts
// The site config
config: Config;
// The data of the taxonomy, from the config
taxonomy: TaxonomyConfig;
// The current full permalink for that page
current_url: String;
// The current path for that page
current_path: String;
// The current term being rendered
term: TaxonomyTerm;
// The lang of the current page
lang: String;
```

A paginated taxonomy term will also get a `paginator` variable; see the
[pagination page](@/templates/pagination.md) for more details.
