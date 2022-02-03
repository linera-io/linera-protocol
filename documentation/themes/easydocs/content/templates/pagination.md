+++
title = "Pagination"
weight = 30
+++

Two things can get paginated: a section and a taxonomy term.

Both kinds get a `paginator` variable of the `Pager` type, on top of the common variables mentioned in the
[overview page](@/templates/overview.md):

```ts
// How many items per pager
paginate_by: Number;
// The base URL for the pagination: section permalink + pagination path
// You can concatenate an integer with that to get a link to a given pagination pager.
base_url: String;
// How many pagers in total
number_pagers: Number;
// Permalink to the first pager
first: String;
// Permalink to the last pager
last: String;
// Permalink to the previous pager, if there is one
previous: String?;
// Permalink to the next pager, if there is one
next: String?;
// All pages for the current pager
pages: Array<Page>;
// Which pager are we on
current_index: Number;
// Total number of pages accross all the pagers
total_pages: Number;
```

A pager is a page of the pagination; if you have 100 pages and paginate_by is set to 10, you will have 10 pagers each
containing 10 pages.

## Section

A paginated section gets the same `section` variable as a normal
[section page](@/templates/pages-sections.md#section-variables)
minus its pages. The pages are instead in `paginator.pages`.

## Taxonomy term

A paginated taxonomy gets two variables aside from the `paginator` variable:

- a `taxonomy` variable of type `TaxonomyConfig`
- a `term` variable of type `TaxonomyTerm`.

See the [taxonomies page](@/templates/taxonomies.md) for a detailed version of the types.
