# Target Playbook

- Use direct target.com PDP links.
- Watch for "pickup" and "ship it" wording in stock marker behavior.
- Start with conservative polling and increase only after stability checks.
- Prefer keyword filters for variant-specific product names.

## Category support

Target monitors support the following categories:

- `pokemon`
- `sports_cards`
- `one_piece`
- `lorcana`

Category-specific parser hooks:

- `sports_cards`: includes `shipping available` as an in-stock marker and prefers `current_retail` price blobs when present.
- `one_piece`: includes `choose store` as a positive marker and treats `limited availability` as an out/limited signal.
- `lorcana`: treats `preorder` as a positive marker and `release date pending` as an unavailable signal.
