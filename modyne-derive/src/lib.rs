extern crate proc_macro;

mod case;
mod entity_def;
mod parsing;
mod projection;
mod symbol;

use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro_derive(EntityDef, attributes(serde))]
pub fn derive_entity_def(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);

    crate::entity_def::generate(input)
        .unwrap_or_else(|err| err.into_compile_error())
        .into()
}

#[proc_macro_derive(Projection, attributes(serde, entity))]
pub fn derive_projection(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);

    crate::projection::generate(input)
        .unwrap_or_else(|err| err.into_compile_error())
        .into()
}
