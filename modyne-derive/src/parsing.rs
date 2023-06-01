use crate::{case::RenameRule, symbol::*};

pub struct ContainerAttrs {
    pub name: Option<syn::LitStr>,
    pub rename_rule: RenameRule,
    pub entity: Option<syn::Path>,
}

impl ContainerAttrs {
    pub fn from_ast(ast: &[syn::Attribute]) -> syn::Result<Self> {
        let mut name = None;
        let mut rename_rule = RenameRule::None;
        let mut entity = None;

        for attr in ast {
            if attr.path() == ENTITY {
                attr.parse_nested_meta(|inner| {
                    if entity.is_some() {
                        return Err(syn::Error::new_spanned(
                            inner.path,
                            "only one entity type can be specified",
                        ));
                    }
                    entity = Some(inner.path);
                    Ok(())
                })?;
            } else if attr.path() == SERDE {
                if let syn::Meta::List(meta) = &attr.meta {
                    if meta.tokens.is_empty() {
                        continue;
                    }
                }

                attr.parse_nested_meta(|meta| {
                    if meta.path == RENAME {
                        name = Some(get_lit_str2(RENAME, RENAME, &meta)?);
                    } else if meta.path == RENAME_ALL {
                        rename_rule = RenameRule::from_str(
                            &get_lit_str2(RENAME_ALL, RENAME_ALL, &meta)?.value(),
                        )
                        .map_err(|err| syn::Error::new_spanned(attr, err))?;
                    } else if meta.input.peek(syn::Token![=]) {
                        let _: syn::Expr = meta.value()?.parse()?;
                    } else if meta.input.lookahead1().peek(syn::token::Paren) {
                        meta.parse_nested_meta(|inner| {
                            let _: syn::Expr = inner.value()?.parse()?;
                            Ok(())
                        })?;
                    }
                    Ok(())
                })?;
            }
        }

        Ok(Self {
            name,
            rename_rule,
            entity,
        })
    }
}

pub fn get_field_names(
    rename_rule: RenameRule,
    data: &syn::DataStruct,
) -> syn::Result<Vec<String>> {
    let mut field_names = Vec::new();

    for field in &data.fields {
        let (flat, name) = field_name_override_from_attrs(&field.attrs)?;

        if flat {
            return Ok(Vec::new());
        }

        let name = if let Some(name) = name {
            name
        } else {
            get_field_name(rename_rule, field.ident.as_ref())?
        };

        field_names.push(name);
    }

    Ok(field_names)
}

fn get_field_name(rename_rule: RenameRule, name: Option<&syn::Ident>) -> syn::Result<String> {
    let name = name
        .ok_or_else(|| syn::Error::new_spanned(name, "expected a named field"))?
        .to_string();

    Ok(rename_rule.apply_to_field(&name))
}

fn field_name_override_from_attrs(attrs: &[syn::Attribute]) -> syn::Result<(bool, Option<String>)> {
    let mut name = None;
    let mut flat = false;

    for attr in attrs {
        if attr.path() != SERDE {
            continue;
        }

        if let syn::Meta::List(meta) = &attr.meta {
            if meta.tokens.is_empty() {
                continue;
            }
        }

        attr.parse_nested_meta(|meta| {
            if meta.path == RENAME {
                name = Some(get_lit_str2(RENAME, RENAME, &meta)?.value());
            } else if meta.path == FLATTEN {
                flat = true;
                // return Err(meta.error("flatten is not currently supported by EntityDef"));
            } else if meta.input.peek(syn::Token![=]) {
                let _: syn::Expr = meta.value()?.parse()?;
            } else if meta.input.lookahead1().peek(syn::token::Paren) {
                meta.parse_nested_meta(|inner| {
                    let _: syn::Expr = inner.value()?.parse()?;
                    Ok(())
                })?;
            }
            Ok(())
        })?;
    }

    Ok((flat, name))
}

pub fn get_lit_str2(
    attr_name: Symbol,
    meta_item_name: Symbol,
    meta: &syn::meta::ParseNestedMeta,
) -> syn::Result<syn::LitStr> {
    let expr: syn::Expr = meta.value()?.parse()?;
    let mut value = &expr;
    while let syn::Expr::Group(e) = value {
        value = &e.expr;
    }
    if let syn::Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Str(lit),
        ..
    }) = value
    {
        Ok(lit.clone())
    } else {
        Err(meta.error(format!(
            "expected serde {} attribute to be a string: `{} = \"...\"`",
            attr_name, meta_item_name
        )))
    }
}
