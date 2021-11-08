# PdmVPages
###### Various small PdmV pages that did not fit anywhere else

## Pages

#### CMS Main Monte Carlo Background Samples - UL ([main_bkg_ul](main_bkg_ul))

It is a monitoring tool designed to monitor progress of the so-called *main physics background* samples widely used by PAGs. Given it is based on dataset names, it can both show status of existing requests as well as point out missing ones.

This web page will also aid the planning and preparation for new PAG Run3 Campaigns to be setup in March of 2022.

#### PdmV ReReco UL ([rereco_ul](rereco_ul))

It is a monitoring tool designed to display ReRECO primary datasets which were (are) being reprocessed under the Ultra Legacy conditions.

Project consists of two tables:
- User table - simplified version that shows only one dataset for each campaign, intended to be used by the majority of regular users
- Full table - table aimed at experts that shows all existing workflows and datasets for each RAW dataset

#### Staging workflows ([transferor_stuckor](transferor_stuckor))

It is a monitoring tool designed to track status of staging input datasets needed for submitted workflows.

## Searching, sorting and sharing

Unless specified otherwise, projects mentioned above use the same searching, sorting and URL sharing approach.

#### Searching
Table entries can be filtered using search fields in the table header. Using search field of a particular column will filter entries based on that attribute, e.g. typing `XYZ` in search field of "Dataset name" column will show entries that have `XYZ` in their dataset name. Search uses regular expressions under the hood, so more complex regex queries are supported. If first symbol of search query is `-` or `!`, an inverse search will be performed, i.e. `-XYZ` and `!XYZ` search of "Dataset name" will omit all entries that have `XYZ` in their dataset name. As a side effect, queries `!*` and `-*` only leave entries with empty values in the given column. Spaces and asterisks (`*`) are treated as wildcards of any number of characters.

#### Sorting
Table entries can be sorted based on values of certain columns. Not all columns can be used for sorting, mouse cursor will change to pointer when hovering over sortable column name. To sort entries based on certain column, click column name in the header. Repeatedly clicking same column name will toggle between ascending and descending sort.

#### Sharing
Search and sort parameters are immediately reflected in the URL, so it can be bookmarked or copied and shared with other people.
