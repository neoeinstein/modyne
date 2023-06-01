extern crate proc_macro;

mod case;
mod entity_def;
mod parsing;
mod projection;
mod symbol;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Derive macro for the `EntityDef` trait
///
/// This macro piggy-backs on the attributes used by the `serde_derive`
/// crate. Note that using `flatten` will result in an empty projection
/// expression, pulling _all_ attributes on the item because this macro
/// cannot identify the field names used in the flattened structure.
#[proc_macro_derive(EntityDef, attributes(serde))]
pub fn derive_entity_def(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);

    crate::entity_def::generate(input)
        .unwrap_or_else(|err| err.into_compile_error())
        .into()
}

/// Derive macro for the `Projection` trait
///
/// Like `EntityDef`, this macro piggy-backs on the attributes used by
/// the `serde_derive` crate. Note that using `flatten` will result in
/// an empty projection expression, pulling _all_ attributes on the item
/// because this macro cannot identify the field names used in the
/// flattened structure.
///
/// Usage of this macro requires specifying the "parent" entity. For
/// example, with `MyEntity`, the projection should have the following
/// attribute: `#[entity(MyEntity)]`
#[proc_macro_derive(Projection, attributes(serde, entity))]
pub fn derive_projection(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);

    crate::projection::generate(input)
        .unwrap_or_else(|err| err.into_compile_error())
        .into()
}
