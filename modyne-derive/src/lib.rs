extern crate proc_macro;

mod case;
mod symbol;

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

use crate::{case::RenameRule, symbol::*};

/// Derive macro for the `EntityDef` trait
///
/// This macro piggy-backs on the attributes used by the `serde_derive`
/// crate. Note that using `flatten` will result in an empty projection
/// expression, pulling _all_ attributes on the item because this macro
/// cannot identify the field names used in the flattened structure.
#[proc_macro_derive(EntityDef, attributes(serde))]
pub fn derive_entity_def(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);

    parse_it(input)
        .unwrap_or_else(|err| err.into_compile_error())
        .into()
}

fn parse_it(input: syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(input, "EntityDef may only be defined on a struct"))
    };

    let cont_attrs = ContainerAttrs::from_ast(&input.attrs)?;
    let field_names = get_field_names(cont_attrs.rename_rule, data)?;

    let name = if let Some(name) = &cont_attrs.name {
        name.value()
    } else {
        RenameRule::SnakeCase.apply_to_variant(&input.ident.to_string())
    };
    let input_ident = &input.ident;

    Ok(quote! {
        impl ::modyne::EntityDef for #input_ident {
            const ENTITY_TYPE: &'static ::modyne::EntityTypeNameRef = ::modyne::EntityTypeNameRef::from_static(#name);
            const PROJECTED_ATTRIBUTES: &'static [&'static str] = &[
                #(#field_names ,)*
            ];
        }
    })
}

struct ContainerAttrs {
    name: Option<syn::LitStr>,
    rename_rule: RenameRule,
}

fn get_field_names(rename_rule: RenameRule, data: &syn::DataStruct) -> syn::Result<Vec<String>> {
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

impl ContainerAttrs {
    fn from_ast(ast: &[syn::Attribute]) -> Result<Self, syn::Error> {
        let mut name = None;
        let mut rename_rule = RenameRule::None;

        for attr in ast {
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
                    name = Some(get_lit_str2(RENAME, RENAME, &meta)?);
                } else if meta.path == RENAME_ALL {
                    rename_rule =
                        RenameRule::from_str(&get_lit_str2(RENAME_ALL, RENAME_ALL, &meta)?.value())
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

        Ok(Self { name, rename_rule })
    }
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

fn get_lit_str2(
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
