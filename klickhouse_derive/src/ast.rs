
use crate::{attr, check};
// use crate::check;
use crate::{ctxt::Ctxt};
use syn;
use syn::Token;
use syn::punctuated::Punctuated;

/// A source data structure annotated with `#[derive(Serialize)]` and/or `#[derive(Deserialize)]`,
/// parsed into an internal representation.
pub struct Container<'a> {
    /// The struct or enum name (without generics).
    pub ident: syn::Ident,
    /// Attributes on the structure, parsed for Serde.
    pub attrs: attr::Container,
    /// The contents of the struct or enum.
    pub data: Struct<'a>,
    /// Any generics on the struct or enum.
    pub generics: &'a syn::Generics,
    /// Original input.
    pub original: &'a syn::DeriveInput,
}

/// The fields of a struct or enum.
///
/// Analogous to `syn::Data`.
pub type Struct<'a> = Vec<Field<'a>>;

/// A field of a struct.
pub struct Field<'a> {
    pub member: syn::Member,
    pub attrs: attr::Field,
    pub ty: &'a syn::Type,
    pub original: &'a syn::Field,
}

impl<'a> Container<'a> {
    /// Convert the raw Syn ast into a parsed container object, collecting errors in `cx`.
    pub fn from_ast(
        cx: &Ctxt,
        item: &'a syn::DeriveInput,
    ) -> Option<Container<'a>> {
        let attrs = attr::Container::from_ast(cx, item);

        let mut data = match &item.data {
            syn::Data::Struct(data) => {
                struct_from_ast(cx, &data.fields, attrs.default())
            }
            syn::Data::Union(_) => {
                cx.error_spanned_by(item, "Klickhouse Row does not support unions");
                return None;
            }
            syn::Data::Enum(_) => {
                cx.error_spanned_by(item, "Klickhouse Row does not support enums");
                return None;
            }
        };

        for field in &mut data {
            field.attrs.rename_by_rules(attrs.rename_all_rule());
        }

        let mut item = Container {
            ident: item.ident.clone(),
            attrs,
            data,
            generics: &item.generics,
            original: item,
        };
        check::check(cx, &mut item);
        Some(item)
    }

}

fn struct_from_ast<'a>(
    cx: &Ctxt,
    fields: &'a syn::Fields,
    container_default: &attr::Default,
) -> Vec<Field<'a>> {
    match fields {
        syn::Fields::Named(fields) => fields_from_ast(cx, &fields.named, container_default),
        syn::Fields::Unnamed(fields) => {
            cx.error_spanned_by(fields, "Klickhouse Row does not support tuple structs");
            vec![]
        },
        syn::Fields::Unit => {
            cx.error_spanned_by(fields, "Klickhouse Row does not support unit structs");
            vec![]
        },
    }
}

fn fields_from_ast<'a>(
    cx: &Ctxt,
    fields: &'a Punctuated<syn::Field, Token![,]>,
    container_default: &attr::Default,
) -> Vec<Field<'a>> {
    fields
        .iter()
        .enumerate()
        .map(|(i, field)| Field {
            member: match &field.ident {
                Some(ident) => syn::Member::Named(ident.clone()),
                None => syn::Member::Unnamed(i.into()),
            },
            attrs: attr::Field::from_ast(cx, i, field, container_default),
            ty: &field.ty,
            original: field,
        })
        .collect()
}