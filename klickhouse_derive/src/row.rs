use crate::ast::{Container, Field};
use crate::ctxt::Ctxt;
use crate::fragment::{Expr, Fragment, Match, Stmts};
use crate::receiver::replace_receiver;
use crate::{attr, bound, dummy};
use proc_macro2::{Span, TokenStream};
use syn::spanned::Spanned;
use syn::{self, Ident, Member};

macro_rules! quote_block {
    ($($tt:tt)*) => {
        $crate::fragment::Fragment::Block(quote!($($tt)*))
    }
}

macro_rules! quote_expr {
    ($($tt:tt)*) => {
        $crate::fragment::Fragment::Expr(quote!($($tt)*))
    }
}

struct Parameters {
    /// Variable holding the value being serialized. Either `self` for local
    /// types or `__self` for remote types.
    self_var: Ident,

    /// Path to the type the impl is for. Either a single `Ident` for local
    /// types or `some::remote::Ident` for remote types. Does not include
    /// generic parameters.
    this: syn::Path,

    /// Generics including any explicit and inferred bounds for the impl.
    generics: syn::Generics,

    /// Type has a repr(packed) attribute.
    is_packed: bool,
}

fn build_generics(cont: &Container) -> syn::Generics {
    let generics = bound::without_defaults(cont.generics);

    let generics = bound::with_where_predicates_from_fields(cont, &generics, attr::Field::bound);

    match cont.attrs.bound() {
        Some(predicates) => bound::with_where_predicates(&generics, predicates),
        None => bound::with_bound(
            cont,
            &generics,
            needs_serialize_bound,
            &[
                &parse_quote!(::klickhouse::FromSql),
                &parse_quote!(::klickhouse::ToSql),
            ],
        ),
    }
}

fn needs_serialize_bound(field: &attr::Field) -> bool {
    !field.skip_serializing() && field.serialize_with().is_none() && field.bound().is_none()
}

impl Parameters {
    fn new(cont: &Container) -> Self {
        let self_var = Ident::new("self", Span::call_site());

        let this = cont.ident.clone().into();

        let is_packed = cont.attrs.is_packed();

        let generics = build_generics(cont);

        Parameters {
            self_var,
            this,
            generics,
            is_packed,
        }
    }
}

pub fn expand_derive_serialize(
    input: &mut syn::DeriveInput,
) -> Result<TokenStream, Vec<syn::Error>> {
    replace_receiver(input);

    let ctxt = Ctxt::new();
    let cont = match Container::from_ast(&ctxt, input) {
        Some(cont) => cont,
        None => return Err(ctxt.check().unwrap_err()),
    };
    ctxt.check()?;

    let ident = &cont.ident;
    let params = Parameters::new(&cont);
    let (impl_generics, ty_generics, where_clause) = params.generics.split_for_impl();
    let serialize_body = Stmts(serialize_body(&cont, &params));
    let deserialize_body = Stmts(deserialize_body(&cont, &params));

    let impl_block = quote! {
        use ::klickhouse::{ToSql as _, FromSql as _};
        #[automatically_derived]
        impl #impl_generics ::klickhouse::Row for #ident #ty_generics #where_clause {
            fn deserialize_row(map: Vec<(&str, &::klickhouse::Type, ::klickhouse::Value)>) -> ::klickhouse::Result<Self> {
                #deserialize_body
            }

            fn serialize_row(self, type_hints: &[&::klickhouse::Type]) -> ::klickhouse::Result<Vec<(::std::borrow::Cow<'static, str>, ::klickhouse::Value)>> {
                #serialize_body
            }
        }
    };

    Ok(dummy::wrap_in_const(impl_block))
}

fn serialize_body(cont: &Container, params: &Parameters) -> Fragment {
    if let Some(type_into) = cont.attrs.type_into() {
        serialize_into(params, type_into)
    } else {
        serialize_struct(params, &cont.data[..], &cont.attrs)
    }
}

fn serialize_into(params: &Parameters, type_into: &syn::Type) -> Fragment {
    let self_var = &params.self_var;
    quote_block! {
        ::klickhouse::Row::serialize_row(
            &::std::convert::Into::<#type_into>::into(#self_var)
        )
    }
}

fn serialize_struct(params: &Parameters, fields: &[Field], cattrs: &attr::Container) -> Fragment {
    assert!(fields.len() as u64 <= u64::from(u32::max_value()));

    serialize_struct_as_struct(params, fields, cattrs)
}

fn serialize_struct_as_struct(
    params: &Parameters,
    fields: &[Field],
    _cattrs: &attr::Container,
) -> Fragment {
    let serialize_fields = serialize_struct_visitor(fields, params);

    quote_block! {
        let mut out = vec![];
        #(#serialize_fields)*
        Ok(out)
    }
}

fn serialize_struct_visitor(fields: &[Field], params: &Parameters) -> Vec<TokenStream> {
    fields
        .iter()
        .filter(|&field| !field.attrs.skip_serializing())
        .enumerate()
        .map(|(i, field)| {
            let member = &field.member;

            let field_expr = get_member(params, member);

            let key_expr = field.attrs.name().name();

            let skip = field
                .attrs
                .skip_serializing_if()
                .map(|path| quote!(#path(&#field_expr)));

            let field_ty = field.ty;
            let ser = match field.attrs.serialize_with() {
                Some(path) => {
                    quote! {
                        out.push((::std::borrow::Cow::Borrowed(#key_expr), #path(#field_expr)?));
                    }
                },
                None => {
                    quote! {
                        out.push((::std::borrow::Cow::Borrowed(#key_expr), <#field_ty as ::klickhouse::ToSql>::to_sql(#field_expr, type_hints.get(#i).copied())?));
                    }
                },
            };

            if let Some(skip) = skip {
                quote! {
                    if !#skip {
                        #ser
                    }
                }
            } else {
                ser
            }
        })
        .collect()
}

fn get_member(params: &Parameters, member: &Member) -> TokenStream {
    let self_var = &params.self_var;
    if params.is_packed {
        quote!({#self_var.#member})
    } else {
        quote!(#self_var.#member)
    }
}

fn deserialize_body(cont: &Container, params: &Parameters) -> Fragment {
    if let Some(type_from) = cont.attrs.type_from() {
        deserialize_from(type_from)
    } else if let Some(type_try_from) = cont.attrs.type_try_from() {
        deserialize_try_from(type_try_from)
    } else {
        deserialize_struct(params, &cont.data[..], &cont.attrs)
    }
}

fn deserialize_from(type_from: &syn::Type) -> Fragment {
    quote_block! {
        ::klickhouse::Result::map(
            <#type_from as ::klickhouse::Row>::deserialize_row(map),
            ::std::convert::From::from)
    }
}

fn deserialize_try_from(type_try_from: &syn::Type) -> Fragment {
    quote_block! {
        ::klickhouse::Result::and_then(
            <#type_try_from as ::klickhouse::Row>::deserialize_row(map),
            |v| ::std::convert::TryFrom::try_from(v).map_err(::std::convert::Into::into))
    }
}

fn deserialize_struct(params: &Parameters, fields: &[Field], cattrs: &attr::Container) -> Fragment {
    let this = &params.this;
    // let (de_impl_generics, de_ty_generics, ty_generics, where_clause) =
    //     split_with_de_lifetime(params);

    let construct = quote!(#this);

    let type_path = construct;

    let visit_map = deserialize_map(&type_path, fields, cattrs);
    let visit_map = Stmts(visit_map);

    quote_block! {
        #visit_map
    }
}

fn field_i(i: usize) -> Ident {
    Ident::new(&format!("__field{}", i), Span::call_site())
}

fn deserialize_map(
    struct_path: &TokenStream,
    fields: &[Field],
    cattrs: &attr::Container,
) -> Fragment {
    // Create the field names for the fields.
    let fields_names: Vec<_> = fields
        .iter()
        .enumerate()
        .map(|(i, field)| (field, field_i(i)))
        .collect();

    // Declare each field that will be deserialized.
    let let_values = fields_names
        .iter()
        .filter(|&&(field, _)| !field.attrs.skip_deserializing())
        .map(|(field, name)| {
            let field_ty = field.ty;
            quote! {
                let mut #name: ::std::option::Option<#field_ty> = ::std::option::Option::None;
            }
        });

    // Match arms to extract a value for a field.
    let mut name_match_arms = Vec::with_capacity(fields_names.len());
    let mut index_match_arms = Vec::with_capacity(fields_names.len());

    fields_names
        .iter()
        .filter(|&&(field, _)| !field.attrs.skip_deserializing())
        .enumerate()
        .for_each(|(index, (field, name))| {
            let deser_name = field.attrs.name().name();

            let visit = match field.attrs.deserialize_with() {
                None => {
                    let field_ty = field.ty;
                    let span = field.original.span();
                    quote_spanned!(span=> <#field_ty as ::klickhouse::FromSql>::from_sql(_type_, _value).map_err(|e| e.with_column_name(#deser_name))?)
                }
                Some(path) => {
                    let span = field.original.span();
                    quote_spanned!(span=> #path(_type_, _value)?)
                }
            };
            name_match_arms.push(quote! {
                #deser_name => {
                    if ::std::option::Option::is_some(&#name) {
                        return ::klickhouse::Result::Err(::klickhouse::KlickhouseError::DuplicateField(#deser_name));
                    }
                    #name = ::std::option::Option::Some(#visit);
                }
            });
            index_match_arms.push(quote! {
                #index => {
                    if ::std::option::Option::is_some(&#name) {
                        return ::klickhouse::Result::Err(::klickhouse::KlickhouseError::DuplicateField(#deser_name));
                    }
                    #name = ::std::option::Option::Some(#visit);
                }
            });
        });

    // Visit ignored values to consume them
    let ignored_arm = if cattrs.deny_unknown_fields() {
        quote! {
            _ => {
                return ::klickhouse::Result::Err(::klickhouse::KlickhouseError::UnknownField(_name));
            }
        }
    } else {
        quote! {
            _ => { }
        }
    };

    let index_match_arm = quote! {
        match _field_index {
            #(#index_match_arms)*
            #ignored_arm
        }
    };

    let match_keys = quote! {
        for (_field_index, (_name, _type_, _value)) in map.into_iter().enumerate() {
            match _name {
                #(#name_match_arms)*
                _ => #index_match_arm,
            }
        }
    };

    let extract_values = fields_names
        .iter()
        .filter(|&&(field, _)| !field.attrs.skip_deserializing())
        .map(|(field, name)| {
            let missing_expr = Match(expr_is_missing(field, cattrs));

            quote! {
                let #name = match #name {
                    ::std::option::Option::Some(#name) => #name,
                    ::std::option::Option::None => #missing_expr
                };
            }
        });

    let result = fields_names.iter().map(|(field, name)| {
        let member = &field.member;
        if field.attrs.skip_deserializing() {
            let value = Expr(expr_is_missing(field, cattrs));
            quote!(#member: #value)
        } else {
            quote!(#member: #name)
        }
    });

    let let_default = match cattrs.default() {
        attr::Default::Default => Some(quote!(
            let __default: Self = ::std::default::Default::default();
        )),
        attr::Default::Path(path) => Some(quote!(
            let __default: Self = #path();
        )),
        attr::Default::None => {
            // We don't need the default value, to prevent an unused variable warning
            // we'll leave the line empty.
            None
        }
    };

    let result = quote!(#struct_path { #(#result),* });

    quote_block! {
        #(#let_values)*

        #match_keys

        #let_default

        #(#extract_values)*

        ::klickhouse::Result::Ok(#result)
    }
}

fn expr_is_missing(field: &Field, cattrs: &attr::Container) -> Fragment {
    match field.attrs.default() {
        attr::Default::Default => {
            let span = field.original.span();
            let func = quote_spanned!(span=> ::std::default::Default::default);
            return quote_expr!(#func());
        }
        attr::Default::Path(path) => {
            return quote_expr!(#path());
        }
        attr::Default::None => { /* below */ }
    }

    match *cattrs.default() {
        attr::Default::Default | attr::Default::Path(_) => {
            let member = &field.member;
            return quote_expr!(__default.#member);
        }
        attr::Default::None => { /* below */ }
    }

    let name = field.attrs.name().name();
    let span = field.original.span();
    let func = quote_spanned!(span=> ::klickhouse::KlickhouseError::MissingField);
    quote_expr! {
        return ::klickhouse::Result::Err(#func(#name))
    }
}
