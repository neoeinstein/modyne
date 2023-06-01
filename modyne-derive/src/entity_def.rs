use quote::quote;

use crate::{
    case::RenameRule,
    parsing::{get_field_names, ContainerAttrs},
};

pub fn generate(input: syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
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
