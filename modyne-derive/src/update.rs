use ::proc_macro2::{Span, TokenStream};
use ::quote::quote;
use ::syn::{DeriveInput, Error};
use quote::quote_spanned;
use syn::LitStr;

//pub fn generate(input: DeriveInput) -> syn::Result<TokenStream> {}

pub fn impl_into_update(input: DeriveInput) -> syn::Result<TokenStream> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "EntityDef may only be defined on a struct",
        ));
    };

    let syn::Fields::Named(fields) = &data.fields else {
        return Err(Error::new(
            Span::call_site(),
            "Expected a `struct` with named fields",
        ));
    };
    let name = input.ident;

    let fields_expanded = fields.named.iter().map(|field| {
        let field_name = field.ident.as_ref().expect("Unreachable");
        let span = field_name.span();
        let field_name_lit = LitStr::new(&field_name.to_string(), span);
        let expr_name_lit = LitStr::new(&format!("#{field_name}"), span);
        let expr_value_lit = LitStr::new(&format!(":{field_name}"), span);
        let expression_lit = LitStr::new(&format!("SET #{field_name} = :{field_name}"), span);
        if is_option(&field.ty) {
            quote_spanned! {
                span =>
                if let Some(#field_name) = &self.#field_name {
                    expr = expr.add_expression(#expression_lit);
                    expr = expr.name(#expr_name_lit, #field_name_lit);
                    expr = expr.value(#expr_value_lit, #field_name);
                }
            }
        } else {
            quote_spanned! {
            span =>
                expr = expr.add_expression(#expression_lit);
                expr = expr.name(#expr_name_lit, #field_name_lit);
                expr = expr.value(#expr_value_lit, #field_name);
            }
        }
    });

    let expanded = quote! {
        impl Into<::modyne::expr::Update> for #name {
            fn into(self) -> ::modyne::expr::Update {
                let mut expr = expr::Update::new("");
                #( #fields_expanded)*
                expr
            }
        }
    };

    Ok(expanded)
}

fn is_option(ty: &syn::Type) -> bool {
    if let syn::Type::Path(syn::TypePath { qself: None, path }) = ty {
        let segments_str = &path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>()
            .join(":");

        let option_segment = ["Option", "std:option:Option", "core:option:Option"]
            .iter()
            .find(|s| segments_str == *s)
            .and_then(|_| path.segments.last());

        return option_segment.is_some();
    }

    false
}
