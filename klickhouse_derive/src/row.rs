use crate::ast::{Container, Field};
use crate::ctxt::Ctxt;
use crate::fragment::{Expr, Fragment, Match, Stmts};
use crate::receiver::replace_receiver;
use crate::{attr, bound, dummy};
use proc_macro2::{Span, TokenStream};
use syn::spanned::Spanned;
use syn::{self, GenericArgument, Ident, Member, PathArguments, Type};

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
    let serialize_length_body = Stmts(serialize_length_body(&cont, &params));
    let const_column_count_fn = format_ident!("__{ident}_column_count_klickhouse");

    let impl_block = quote! {
        #[doc(hidden)]
        const fn #const_column_count_fn() -> ::std::option::Option<usize> {
            #serialize_length_body
        }

        use ::klickhouse::{ToSql as _, FromSql as _};
        #[automatically_derived]
        impl #impl_generics ::klickhouse::Row for #ident #ty_generics #where_clause {
            const COLUMN_COUNT: ::std::option::Option<usize> = #const_column_count_fn();

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

fn unwrap_vec_type(type_: &Type) -> Option<&Type> {
    match type_ {
        Type::Path(type_) => {
            if type_.qself.is_some()
                || type_.path.leading_colon.is_some()
                || type_.path.segments.len() != 1
            {
                return None;
            }
            let segment = &type_.path.segments[0];
            if &*segment.ident.to_string() != "Vec" {
                return None;
            }
            match &segment.arguments {
                PathArguments::AngleBracketed(a) => {
                    if a.args.len() != 1 {
                        return None;
                    }
                    let arg = &a.args[0];
                    match arg {
                        GenericArgument::Type(t) => Some(t),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn serialize_length_body(cont: &Container, _params: &Parameters) -> Fragment {
    if let Some(_type_into) = cont.attrs.type_into() {
        Fragment::Expr(quote! { None })
    } else {
        let base_length = cont
            .data
            .iter()
            .filter(|&field| !field.attrs.skip_serializing() && !field.attrs.nested())
            .count();
        let mut total = quote! { #base_length };
        for field in cont
            .data
            .iter()
            .filter(|&field| !field.attrs.skip_serializing() && field.attrs.nested())
        {
            let field_ty = unwrap_vec_type(&field.ty).expect("invalid non-Vec nested type");
            total = quote! { match <#field_ty as ::klickhouse::Row>::COLUMN_COUNT { Some(x) => #total + x, None => return None, } }
        }
        Fragment::Expr(quote! { Some(#total) })
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
        let mut field_index: usize = 0;
        #(#serialize_fields)*
        Ok(out)
    }
}

fn serialize_struct_visitor(fields: &[Field], params: &Parameters) -> Vec<TokenStream> {
    fields
        .iter()
        .filter(|&field| !field.attrs.skip_serializing())
        .map(|field| {
            let member = &field.member;

            let field_expr = get_member(params, member);

            let key_expr = field.attrs.name().name();

            let field_ty = &field.ty;
            match field.attrs.serialize_with() {
                Some(path) => {
                    quote! {
                        out.push((::std::borrow::Cow::Borrowed(#key_expr), #path(#field_expr)?));
                    }
                },
                None => {
                    if field.attrs.nested() {
                        let field_ty = unwrap_vec_type(&field.ty).expect("invalid non-Vec nested type");
                        quote! {
                            {
                                let inner_length = <#field_ty as ::klickhouse::Row>::COLUMN_COUNT.expect("nested structure must have known length");
                                let type_hints = type_hints.get(field_index..(field_index + inner_length)).unwrap_or_default().into_iter().map(|x| x.unarray()).collect::<::std::option::Option<::std::vec::Vec<_>>>().unwrap_or_default();
                                field_index += inner_length;
                                let mut outputs: ::std::vec::Vec<(::std::option::Option<::std::borrow::Cow<str>>, ::std::vec::Vec<::klickhouse::Value>)> = ::std::vec::Vec::with_capacity(inner_length);
                                for _ in 0..inner_length {
                                    outputs.push((None, ::std::vec::Vec::new()));
                                }
                                for row in #field_expr.into_iter() {
                                    let columns = <#field_ty as ::klickhouse::Row>::serialize_row(row, &type_hints[..])?;
                                    assert_eq!(columns.len(), inner_length);
                                    for (i, (name, value)) in columns.into_iter().enumerate() {
                                        if outputs[i].0.is_none()  {
                                            outputs[i].0 = Some(format!("{}.{}", #key_expr, name).into());
                                        }
                                        outputs[i].1.push(value);
                                    }
                                }
                                for (name, values) in outputs {
                                    out.push((name.unwrap_or_default(), ::klickhouse::Value::Array(values)));
                                }
                            }
                        }
                    } else {
                        quote! {
                            out.push((::std::borrow::Cow::Borrowed(#key_expr), <#field_ty as ::klickhouse::ToSql>::to_sql(#field_expr, type_hints.get(field_index).copied())?));
                            field_index += 1;
                        }
                    }
                },
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

    let mut nested_temp_decls = vec![];
    let mut nested_rectify = vec![];

    let mut current_index = quote! { 0usize };
    fields_names
        .iter()
        .filter(|&&(field, _)| !field.attrs.skip_deserializing())
        .for_each(|(field, name)| {
            let deser_name = field.attrs.name().name();
            let local_index = current_index.clone();
            let span = field.original.span();

            if field.attrs.nested() {
                let deser_name_dotted = format!("{deser_name}.");
                let deser_name_ext = format_ident!("__ext_{deser_name}");
                let deser_name_ext_iter = format_ident!("__ext_{deser_name}_iter");
                let deser_name_ext_len = format_ident!("__ext_{deser_name}_len");
                let field_ty = unwrap_vec_type(&field.ty).expect("invalid non-Vec nested type");
                let size_field = format_ident!("__{deser_name}_size");
                current_index = quote! { #current_index + #size_field };

                nested_temp_decls.push(quote_spanned! { span=>
                    let #size_field = <#field_ty as ::klickhouse::Row>::COLUMN_COUNT.expect("nested structure must have known length");
                    let mut #deser_name_ext: Vec<(&str, &::klickhouse::Type)> = Vec::with_capacity(#size_field);
                    let mut #deser_name_ext_iter: Vec<::std::vec::IntoIter<::klickhouse::Value>> = Vec::with_capacity(#size_field);
                    let mut #deser_name_ext_len: usize = 0;
                });
                name_match_arms.push(quote_spanned! { span=>
                    full_name if full_name.starts_with(#deser_name_dotted) => {
                        let values = _value.unarray().ok_or_else(|| ::klickhouse::KlickhouseError::UnexpectedTypeWithColumn(::std::borrow::Cow::Owned(full_name.to_string()), _type_.clone()))?;
                        if #deser_name_ext.is_empty() {
                            #deser_name_ext_len = values.len();
                        } else {
                            if #deser_name_ext_len != values.len() {
                                return ::klickhouse::Result::Err(::klickhouse::KlickhouseError::DeserializeError(format!("invalid length for nested columns, mismatches previous column {}: {} != {}", _name, #deser_name_ext_len, values.len())));
                            }
                        }
                        #deser_name_ext.push((full_name.strip_prefix(#deser_name_dotted).unwrap(), _type_.unarray().ok_or_else(|| ::klickhouse::KlickhouseError::UnexpectedTypeWithColumn(::std::borrow::Cow::Owned(full_name.to_string()), _type_.clone()))?));
                        #deser_name_ext_iter.push(values.into_iter());
                    }
                });
                index_match_arms.push(quote_spanned! { span=>
                    x if x >= (#local_index) && x < (#current_index) => {
                        let values = _value.unarray().ok_or_else(|| ::klickhouse::KlickhouseError::UnexpectedTypeWithColumn(::std::borrow::Cow::Owned(_name.to_string()), _type_.clone()))?;
                        if #deser_name_ext.is_empty() {
                            #deser_name_ext_len = values.len();
                        } else {
                            if #deser_name_ext_len != values.len() {
                                return ::klickhouse::Result::Err(::klickhouse::KlickhouseError::DeserializeError(format!("invalid length for nested columns, mismatches previous column {}: {} != {}", _name, #deser_name_ext_len, values.len())));
                            }
                        }
                        #deser_name_ext.push((_name, _type_.unarray().ok_or_else(|| ::klickhouse::KlickhouseError::UnexpectedTypeWithColumn(::std::borrow::Cow::Owned(_name.to_string()), _type_.clone()))?));
                        #deser_name_ext_iter.push(values.into_iter());
                    }
                });
                nested_rectify.push(quote_spanned! { span=>
                    {
                        #name = ::std::option::Option::Some(::std::vec::Vec::with_capacity(#deser_name_ext_len));
                        'outer: loop {
                            let mut temp = ::std::vec::Vec::with_capacity(#size_field);
                            for (name, type_) in #deser_name_ext.iter() {
                                temp.push((*name, *type_, ::klickhouse::Value::Null));
                            }
                            for (i, value) in #deser_name_ext_iter.iter_mut().enumerate() {
                                match value.next() {
                                    None => break 'outer,
                                    Some(x) => temp[i].2 = x,
                                }
                            }
                            #name.as_mut().unwrap().push(<#field_ty as ::klickhouse::Row>::deserialize_row(temp)?);
                        }
                    }
                });
                return;
            } else {
                current_index = quote! { #current_index + 1usize };
            }

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
            name_match_arms.push(quote_spanned! { span=>
                #deser_name => {
                    if ::std::option::Option::is_some(&#name) {
                        return ::klickhouse::Result::Err(::klickhouse::KlickhouseError::DuplicateField(#deser_name));
                    }
                    #name = ::std::option::Option::Some(#visit);
                }
            });
            index_match_arms.push(quote_spanned! { span=>
                x if x == (#local_index) => {
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
        #[allow(unused_comparisons)]
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
        #(#nested_temp_decls)*

        #match_keys

        #(#nested_rectify)*

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
