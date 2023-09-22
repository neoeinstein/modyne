use quote::quote;

use crate::parsing::{get_field_names, ContainerAttrs};

pub fn generate(input: syn::DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "EntityDef may only be defined on a struct",
        ));
    };

    let cont_attrs = ContainerAttrs::from_ast(&input.attrs)?;
    let field_names = get_field_names(cont_attrs.rename_rule, data)?;
    let input_ident = &input.ident;
    let entity_type = cont_attrs.entity.as_ref().ok_or_else(|| {
        syn::Error::new_spanned(
            &input,
            "an entity type declaration is required with #[entity(<Entity>)]",
        )
    })?;

    Ok(quote! {
        impl ::modyne::Projection for #input_ident {
            type Entity = #entity_type;
            const PROJECTED_ATTRIBUTES: &'static [&'static str] = &[
                #(#field_names ,)*
            ];
        }

        /// Verify that the projection only contains attributes from the related entity
        ///
        /// This does not guarantee that the types are right, but helps avoid unintended
        /// name mis-matches.
        const _: () = {
            let mut missing: Option<&str> = None;
            let mut i_arr = <#input_ident as ::modyne::Projection>::PROJECTED_ATTRIBUTES;
            while let Some((i, rest)) = i_arr.split_first() {
                i_arr = rest;
                let mut found = false;

                let mut j_arr = <<#input_ident as ::modyne::Projection>::Entity as ::modyne::EntityDef>::PROJECTED_ATTRIBUTES;
                if j_arr.is_empty() {
                    // The parent entity was using flatten! We can't identify missing elements
                    break;
                }

                'spot: while let Some((j, rest)) = j_arr.split_first() {
                    j_arr = rest;

                    if i.len() != j.len() {
                        continue;
                    }

                    let mut l_arr = i.as_bytes();
                    let mut r_arr = j.as_bytes();
                    loop {
                        match (l_arr.split_first(), r_arr.split_first()) {
                            (Some((&l, l_rest)), Some((&r, r_rest))) => {
                                l_arr = l_rest;
                                r_arr = r_rest;

                                match l.abs_diff(*&r) {
                                    0 => {}
                                    _ => continue 'spot,
                                }
                            }
                            (None, None) => {
                                found = true;
                                break 'spot;
                            }
                            _ => continue 'spot,
                        }
                    }
                }

                if !found {
                    missing = Some(i);
                    break;
                }
            }

            if let Some(missing) = missing {
                panic!("projection contains attribute not found in entity");
            }
        };
    })
}
