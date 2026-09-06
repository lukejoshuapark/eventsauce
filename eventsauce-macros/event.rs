use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{parse_macro_input, Fields, Ident, Item, ItemEnum, ItemStruct};

fn derive_serde() -> TokenStream2 {
    if cfg!(feature = "serde") {
        quote! { #[derive(::serde::Serialize, ::serde::Deserialize)] }
    } else {
        quote! {}
    }
}

fn occurred_at_serde_attr() -> TokenStream2 {
    if cfg!(feature = "serde") {
        quote! { #[serde(with = "::time::serde::rfc3339::option")] }
    } else {
        quote! {}
    }
}

fn serde_tag_attr() -> TokenStream2 {
    if cfg!(feature = "serde") {
        quote! { #[serde(tag = "type")] }
    } else {
        quote! {}
    }
}

pub fn event(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as Item);

    match input {
        Item::Struct(item_struct) => event_struct(attr, item_struct),
        Item::Enum(item_enum) => event_enum(attr, item_enum),
        _ => panic!("#[event] can only be applied to a struct or an enum")
    }
}

fn event_struct(attr: TokenStream, item_struct: ItemStruct) -> TokenStream {
    let stream_id_field = parse_macro_input!(attr as Ident);

    let ItemStruct { attrs, vis, ident, generics, fields, .. } = item_struct;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let Fields::Named(fields_named) = fields else {
        panic!("#[event] can only be applied to a struct with named fields");
    };

    let field_idents: Vec<&Ident> = fields_named.named.iter()
        .map(|f| f.ident.as_ref().unwrap())
        .collect();

    let field_types: Vec<&syn::Type> = fields_named.named.iter()
        .map(|f| &f.ty)
        .collect();

    let field_vis: Vec<&syn::Visibility> = fields_named.named.iter()
        .map(|f| &f.vis)
        .collect();

    let field_attrs: Vec<&Vec<syn::Attribute>> = fields_named.named.iter()
        .map(|f| &f.attrs)
        .collect();

    let derive_serde = derive_serde();
    let occurred_at_serde_attr = occurred_at_serde_attr();

    let expanded = quote! {
        #derive_serde
        #(#attrs)*
        #[derive(Debug)]
        #vis struct #ident #ty_generics #where_clause {
            pub id: ::uuid::Uuid,
            pub #stream_id_field: ::uuid::Uuid,
            #occurred_at_serde_attr
            pub occurred_at: ::core::option::Option<::time::OffsetDateTime>,
            pub sequence_number: ::core::option::Option<u32>,
            #(
                #(#field_attrs)*
                #field_vis #field_idents: #field_types
            ),*
        }

        impl #impl_generics ::eventsauce::Event for #ident #ty_generics #where_clause {
            fn id(&self) -> ::uuid::Uuid { self.id }
            fn stream_id(&self) -> ::uuid::Uuid { self.#stream_id_field }

            fn occurred_at(&self) -> ::core::option::Option<::time::OffsetDateTime> { self.occurred_at }
            fn set_occurred_at(&mut self, timestamp: ::time::OffsetDateTime) { self.occurred_at = ::core::option::Option::Some(timestamp); }

            fn sequence_number(&self) -> ::core::option::Option<u32> { self.sequence_number }
            fn set_sequence_number(&mut self, sequence_number: u32) { self.sequence_number = ::core::option::Option::Some(sequence_number); }
        }

        impl #impl_generics #ident #ty_generics #where_clause {
            pub fn new(id: ::uuid::Uuid, #stream_id_field: ::uuid::Uuid, #(#field_idents: #field_types),*) -> Self {
                Self {
                    id,
                    #stream_id_field,
                    occurred_at: ::core::option::Option::None,
                    sequence_number: ::core::option::Option::None,
                    #(#field_idents),*
                }
            }
        }
    };

    expanded.into()
}

fn event_enum(attr: TokenStream, item_enum: ItemEnum) -> TokenStream {
    if !attr.is_empty() {
        panic!("#[event] takes no arguments when applied to an enum");
    }

    let ItemEnum { attrs, vis, ident, generics, variants, .. } = item_enum;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let variant_idents: Vec<&Ident> = variants.iter()
        .map(|v| &v.ident)
        .collect();

    let inner_types: Vec<&syn::Type> = variants.iter()
        .map(|v| {
            let Fields::Unnamed(fields_unnamed) = &v.fields else {
                panic!("#[event] enum variants must be tuple variants wrapping a single event type");
            };

            if fields_unnamed.unnamed.len() != 1 {
                panic!("#[event] enum variants must wrap exactly one event type");
            }

            &fields_unnamed.unnamed.first().unwrap().ty
        })
        .collect();

    let creation_ident = variant_idents.first()
        .expect("#[event] enum must have at least one variant");

    let creation_type = inner_types.first().unwrap();

    let derive_serde = derive_serde();
    let serde_tag_attr = serde_tag_attr();

    let expanded = quote! {
        #derive_serde
        #serde_tag_attr
        #(#attrs)*
        #[derive(Debug)]
        #vis enum #ident #ty_generics #where_clause {
            #(#variant_idents(#inner_types)),*
        }

        impl #impl_generics ::eventsauce::Event for #ident #ty_generics #where_clause {
            fn id(&self) -> ::uuid::Uuid {
                match self {
                    #(#ident::#variant_idents(event) => ::eventsauce::Event::id(event)),*
                }
            }

            fn stream_id(&self) -> ::uuid::Uuid {
                match self {
                    #(#ident::#variant_idents(event) => ::eventsauce::Event::stream_id(event)),*
                }
            }

            fn occurred_at(&self) -> ::core::option::Option<::time::OffsetDateTime> {
                match self {
                    #(#ident::#variant_idents(event) => ::eventsauce::Event::occurred_at(event)),*
                }
            }

            fn set_occurred_at(&mut self, timestamp: ::time::OffsetDateTime) {
                match self {
                    #(#ident::#variant_idents(event) => ::eventsauce::Event::set_occurred_at(event, timestamp)),*
                }
            }

            fn sequence_number(&self) -> ::core::option::Option<u32> {
                match self {
                    #(#ident::#variant_idents(event) => ::eventsauce::Event::sequence_number(event)),*
                }
            }

            fn set_sequence_number(&mut self, sequence_number: u32) {
                match self {
                    #(#ident::#variant_idents(event) => ::eventsauce::Event::set_sequence_number(event, sequence_number)),*
                }
            }
        }

        #(
            impl #impl_generics ::core::convert::From<#inner_types> for #ident #ty_generics #where_clause {
                fn from(event: #inner_types) -> Self {
                    #ident::#variant_idents(event)
                }
            }
        )*

        impl #impl_generics ::core::convert::TryFrom<#ident #ty_generics> for #creation_type #where_clause {
            type Error = ::eventsauce::StateError;

            fn try_from(value: #ident #ty_generics) -> ::core::result::Result<Self, Self::Error> {
                match value {
                    #ident::#creation_ident(event) => ::core::result::Result::Ok(event),
                    _ => ::core::result::Result::Err(::eventsauce::StateError::FirstEventIsNotCreationEvent)
                }
            }
        }
    };

    expanded.into()
}
