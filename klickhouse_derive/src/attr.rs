use crate::ctxt::Ctxt;
use crate::respan::respan;
use crate::symbol::*;
use proc_macro2::{Span, TokenStream, TokenTree};
use quote::ToTokens;
use syn::parse::{self, Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Expr, Ident, Meta};

// This module handles parsing of `#[klickhouse(...)]` attributes. The entrypoints
// are `attr::Container::from_ast`, `attr::Variant::from_ast`, and
// `attr::Field::from_ast`. Each returns an instance of the corresponding
// struct. Note that none of them return a Result. Unrecognized, malformed, or
// duplicated attributes result in a span_err but otherwise are ignored. The
// user will see errors simultaneously for all bad attributes in the crate
// rather than just the first.

pub use crate::case::RenameRule;

struct Attr<'c, T> {
    cx: &'c Ctxt,
    name: Symbol,
    tokens: TokenStream,
    value: Option<T>,
}

impl<'c, T> Attr<'c, T> {
    fn none(cx: &'c Ctxt, name: Symbol) -> Self {
        Attr {
            cx,
            name,
            tokens: TokenStream::new(),
            value: None,
        }
    }

    fn set<A: ToTokens>(&mut self, obj: A, value: T) {
        let tokens = obj.into_token_stream();

        if self.value.is_some() {
            self.cx.error_spanned_by(
                tokens,
                format!("duplicate klickhouse attribute `{}`", self.name),
            );
        } else {
            self.tokens = tokens;
            self.value = Some(value);
        }
    }

    fn set_opt<A: ToTokens>(&mut self, obj: A, value: Option<T>) {
        if let Some(value) = value {
            self.set(obj, value);
        }
    }

    fn set_if_none(&mut self, value: T) {
        if self.value.is_none() {
            self.value = Some(value);
        }
    }

    fn get(self) -> Option<T> {
        self.value
    }
}

struct BoolAttr<'c>(Attr<'c, ()>);

impl<'c> BoolAttr<'c> {
    fn none(cx: &'c Ctxt, name: Symbol) -> Self {
        BoolAttr(Attr::none(cx, name))
    }

    fn set_true<A: ToTokens>(&mut self, obj: A) {
        self.0.set(obj, ());
    }

    fn get(&self) -> bool {
        self.0.value.is_some()
    }
}

pub struct Name {
    name: String,
    renamed: bool,
}

#[allow(deprecated)]
fn unraw(ident: &Ident) -> String {
    // str::trim_start_matches was added in 1.30, trim_left_matches deprecated
    // in 1.33. We currently support rustc back to 1.15 so we need to continue
    // to use the deprecated one.
    ident.to_string().trim_left_matches("r#").to_owned()
}

impl Name {
    fn from_attrs(source_name: String, rename: Attr<String>) -> Name {
        let rename = rename.get();
        Name {
            renamed: rename.is_some(),
            name: rename.unwrap_or_else(|| source_name.clone()),
        }
    }

    /// Return the container name for the container when serializing.
    pub fn name(&self) -> String {
        self.name.clone()
    }
}

/// Represents struct or enum attribute information.
pub struct Container {
    deny_unknown_fields: bool,
    default: Default,
    rename_all_rule: RenameRule,
    bound: Option<Vec<syn::WherePredicate>>,
    type_from: Option<syn::Type>,
    type_try_from: Option<syn::Type>,
    type_into: Option<syn::Type>,
    is_packed: bool,
}

impl Container {
    /// Extract out the `#[klickhouse(...)]` attributes from an item.
    pub fn from_ast(cx: &Ctxt, item: &syn::DeriveInput) -> Self {
        let mut rename = Attr::none(cx, RENAME);
        let mut deny_unknown_fields = BoolAttr::none(cx, DENY_UNKNOWN_FIELDS);
        let mut default = Attr::none(cx, DEFAULT);
        let mut rename_all_rule = Attr::none(cx, RENAME_ALL);
        let mut bound = Attr::none(cx, BOUND);
        let mut type_from = Attr::none(cx, FROM);
        let mut type_try_from = Attr::none(cx, TRY_FROM);
        let mut type_into = Attr::none(cx, INTO);

        for meta_item in item
            .attrs
            .iter()
            .flat_map(|attr| get_klickhouse_meta_items(cx, attr))
            .flatten()
        {
            match &meta_item {
                // Parse `#[klickhouse(rename = "foo")]`
                Meta::NameValue(m) if m.path == RENAME => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(s) = get_lit_str(cx, RENAME, &expr_lit.lit) {
                        rename.set(&m.path, s.value());
                    }
                }

                // Parse `#[klickhouse(rename_all = "foo")]`
                Meta::NameValue(m) if m.path == RENAME_ALL => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(s) = get_lit_str(cx, RENAME_ALL, &expr_lit.lit) {
                        match RenameRule::from_str(&s.value()) {
                            Ok(rename_rule) => {
                                rename_all_rule.set(&m.path, rename_rule);
                            }
                            Err(err) => cx.error_spanned_by(s, err),
                        }
                    }
                }

                // Parse `#[klickhouse(deny_unknown_fields)]`
                Meta::Path(word) if word == DENY_UNKNOWN_FIELDS => {
                    deny_unknown_fields.set_true(word);
                }

                // Parse `#[klickhouse(default)]`
                Meta::Path(word) if word == DEFAULT => match &item.data {
                    syn::Data::Struct(syn::DataStruct { fields, .. }) => match fields {
                        syn::Fields::Named(_) => {
                            default.set(word, Default::Default);
                        }
                        syn::Fields::Unnamed(_) | syn::Fields::Unit => cx.error_spanned_by(
                            fields,
                            "#[klickhouse(default)] can only be used on structs with named fields",
                        ),
                    },
                    syn::Data::Enum(syn::DataEnum { enum_token, .. }) => cx.error_spanned_by(
                        enum_token,
                        "#[klickhouse(default)] can only be used on structs with named fields",
                    ),
                    syn::Data::Union(syn::DataUnion { union_token, .. }) => cx.error_spanned_by(
                        union_token,
                        "#[klickhouse(default)] can only be used on structs with named fields",
                    ),
                },

                // Parse `#[klickhouse(default = "...")]`
                Meta::NameValue(m) if m.path == DEFAULT => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(path) = parse_lit_into_expr_path(cx, DEFAULT, &expr_lit.lit) {
                        match &item.data {
                            syn::Data::Struct(syn::DataStruct { fields, .. }) => {
                                match fields {
                                    syn::Fields::Named(_) => {
                                        default.set(&m.path, Default::Path(path));
                                    }
                                    syn::Fields::Unnamed(_) | syn::Fields::Unit => cx
                                        .error_spanned_by(
                                            fields,
                                            "#[klickhouse(default = \"...\")] can only be used on structs with named fields",
                                        ),
                                }
                            }
                            syn::Data::Enum(syn::DataEnum { enum_token, .. }) => cx
                                .error_spanned_by(
                                    enum_token,
                                    "#[klickhouse(default = \"...\")] can only be used on structs with named fields",
                                ),
                            syn::Data::Union(syn::DataUnion {
                                union_token, ..
                            }) => cx.error_spanned_by(
                                union_token,
                                "#[klickhouse(default = \"...\")] can only be used on structs with named fields",
                            ),
                        }
                    }
                }

                // Parse `#[klickhouse(bound = "T: SomeBound")]`
                Meta::NameValue(m) if m.path == BOUND => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(where_predicates) =
                        parse_lit_into_where(cx, BOUND, BOUND, &expr_lit.lit)
                    {
                        bound.set(&m.path, where_predicates.clone());
                    }
                }

                // Parse `#[klickhouse(from = "Type")]
                Meta::NameValue(m) if m.path == FROM => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(from_ty) = parse_lit_into_ty(cx, FROM, &expr_lit.lit) {
                        type_from.set_opt(&m.path, Some(from_ty));
                    }
                }

                // Parse `#[klickhouse(try_from = "Type")]
                Meta::NameValue(m) if m.path == TRY_FROM => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(try_from_ty) = parse_lit_into_ty(cx, TRY_FROM, &expr_lit.lit) {
                        type_try_from.set_opt(&m.path, Some(try_from_ty));
                    }
                }

                // Parse `#[klickhouse(into = "Type")]
                Meta::NameValue(m) if m.path == INTO => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(into_ty) = parse_lit_into_ty(cx, INTO, &expr_lit.lit) {
                        type_into.set_opt(&m.path, Some(into_ty));
                    }
                }

                meta_item => {
                    let path = meta_item
                        .path()
                        .into_token_stream()
                        .to_string()
                        .replace(' ', "");
                    cx.error_spanned_by(
                        meta_item.path(),
                        format!("unknown klickhouse container attribute `{}`", path),
                    );
                }
            }
        }

        let mut is_packed = false;
        for attr in &item.attrs {
            if attr.path().is_ident("repr") {
                let _ = attr.parse_args_with(|input: ParseStream| {
                    while let Some(token) = input.parse()? {
                        if let TokenTree::Ident(ident) = token {
                            is_packed |= ident == "packed";
                        }
                    }
                    Ok(())
                });
            }
        }

        Container {
            deny_unknown_fields: deny_unknown_fields.get(),
            default: default.get().unwrap_or(Default::None),
            rename_all_rule: rename_all_rule.get().unwrap_or(RenameRule::None),
            bound: bound.get(),
            type_from: type_from.get(),
            type_try_from: type_try_from.get(),
            type_into: type_into.get(),
            is_packed,
        }
    }

    pub fn rename_all_rule(&self) -> &RenameRule {
        &self.rename_all_rule
    }

    pub fn deny_unknown_fields(&self) -> bool {
        self.deny_unknown_fields
    }

    pub fn default(&self) -> &Default {
        &self.default
    }

    pub fn bound(&self) -> Option<&[syn::WherePredicate]> {
        self.bound.as_ref().map(|vec| &vec[..])
    }

    pub fn type_from(&self) -> Option<&syn::Type> {
        self.type_from.as_ref()
    }

    pub fn type_try_from(&self) -> Option<&syn::Type> {
        self.type_try_from.as_ref()
    }

    pub fn type_into(&self) -> Option<&syn::Type> {
        self.type_into.as_ref()
    }

    pub fn is_packed(&self) -> bool {
        self.is_packed
    }
}

/// Represents field attribute information
pub struct Field {
    name: Name,
    skip_serializing: bool,
    skip_deserializing: bool,
    default: Default,
    serialize_with: Option<syn::ExprPath>,
    deserialize_with: Option<syn::ExprPath>,
    bound: Option<Vec<syn::WherePredicate>>,
    nested: bool,
    flatten: bool,
}

#[allow(clippy::enum_variant_names)]
/// Represents the default to use for a field when deserializing.
pub enum Default {
    /// Field must always be specified because it does not have a default.
    None,
    /// The default is given by `std::default::Default::default()`.
    Default,
    /// The default is given by this function.
    Path(syn::ExprPath),
}

impl Field {
    /// Extract out the `#[klickhouse(...)]` attributes from a struct field.
    pub fn from_ast(
        cx: &Ctxt,
        index: usize,
        field: &syn::Field,
        container_default: &Default,
    ) -> Self {
        let mut rename = Attr::none(cx, RENAME);
        let mut nested = BoolAttr::none(cx, NESTED);
        let mut skip_serializing = BoolAttr::none(cx, SKIP_SERIALIZING);
        let mut skip_deserializing = BoolAttr::none(cx, SKIP_DESERIALIZING);
        let mut flatten = BoolAttr::none(cx, FLATTEN);
        let mut default = Attr::none(cx, DEFAULT);
        let mut serialize_with = Attr::none(cx, SERIALIZE_WITH);
        let mut deserialize_with = Attr::none(cx, DESERIALIZE_WITH);
        let mut bound = Attr::none(cx, BOUND);

        let ident = match &field.ident {
            Some(ident) => unraw(ident),
            None => index.to_string(),
        };

        for meta_item in field
            .attrs
            .iter()
            .flat_map(|attr| get_klickhouse_meta_items(cx, attr))
            .flatten()
        {
            match &meta_item {
                // Parse `#[klickhouse(rename = "foo")]`
                Meta::NameValue(m) if m.path == RENAME => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(s) = get_lit_str(cx, RENAME, &expr_lit.lit) {
                        rename.set(&m.path, s.value());
                    }
                }

                // Parse `#[klickhouse(default)]`
                Meta::Path(word) if word == DEFAULT => {
                    default.set(word, Default::Default);
                }

                // Parse `#[klickhouse(default = "...")]`
                Meta::NameValue(m) if m.path == DEFAULT => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(path) = parse_lit_into_expr_path(cx, DEFAULT, &expr_lit.lit) {
                        default.set(&m.path, Default::Path(path));
                    }
                }

                // Parse `#[klickhouse(skip_serializing)]`
                Meta::Path(word) if word == SKIP_SERIALIZING => {
                    skip_serializing.set_true(word);
                }

                // Parse `#[klickhouse(nested)]`
                Meta::Path(word) if word == NESTED => {
                    nested.set_true(word);
                }

                // Parse `#[klickhouse(flatten)]`
                Meta::Path(word) if word == FLATTEN => {
                    flatten.set_true(word);
                }

                // Parse `#[klickhouse(skip_deserializing)]`
                Meta::Path(word) if word == SKIP_DESERIALIZING => {
                    skip_deserializing.set_true(word);
                }

                // Parse `#[klickhouse(skip)]`
                Meta::Path(word) if word == SKIP => {
                    skip_serializing.set_true(word);
                    skip_deserializing.set_true(word);
                }

                // Parse `#[klickhouse(serialize_with = "...")]`
                Meta::NameValue(m) if m.path == SERIALIZE_WITH => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(path) = parse_lit_into_expr_path(cx, SERIALIZE_WITH, &expr_lit.lit) {
                        serialize_with.set(&m.path, path);
                    }
                }

                // Parse `#[klickhouse(deserialize_with = "...")]`
                Meta::NameValue(m) if m.path == DESERIALIZE_WITH => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(path) = parse_lit_into_expr_path(cx, DESERIALIZE_WITH, &expr_lit.lit)
                    {
                        deserialize_with.set(&m.path, path);
                    }
                }

                // Parse `#[klickhouse(with = "...")]`
                Meta::NameValue(m) if m.path == WITH => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(path) = parse_lit_into_expr_path(cx, WITH, &expr_lit.lit) {
                        let mut ser_path = path.clone();
                        ser_path
                            .path
                            .segments
                            .push(Ident::new("to_sql", Span::call_site()).into());
                        serialize_with.set(&m.path, ser_path);
                        let mut de_path = path;
                        de_path
                            .path
                            .segments
                            .push(Ident::new("from_sql", Span::call_site()).into());
                        deserialize_with.set(&m.path, de_path);
                    }
                }

                // Parse `#[klickhouse(bound = "T: SomeBound")]`
                Meta::NameValue(m) if m.path == BOUND => {
                    let Expr::Lit(expr_lit) = &m.value else {
                        continue;
                    };

                    if let Ok(where_predicates) =
                        parse_lit_into_where(cx, BOUND, BOUND, &expr_lit.lit)
                    {
                        bound.set(&m.path, where_predicates.clone());
                    }
                }

                meta_item => {
                    let path = meta_item
                        .path()
                        .into_token_stream()
                        .to_string()
                        .replace(' ', "");
                    cx.error_spanned_by(
                        meta_item.path(),
                        format!("unknown klickhouse field attribute `{}`", path),
                    );
                }
            }
        }

        // Is skip_deserializing, initialize the field to Default::default() unless a
        // different default is specified by `#[klickhouse(default = "...")]` on
        // ourselves or our container (e.g. the struct we are in).
        if let Default::None = *container_default {
            if skip_deserializing.0.value.is_some() {
                default.set_if_none(Default::Default);
            }
        }

        Field {
            name: Name::from_attrs(ident, rename),
            skip_serializing: skip_serializing.get(),
            skip_deserializing: skip_deserializing.get(),
            default: default.get().unwrap_or(Default::None),
            serialize_with: serialize_with.get(),
            deserialize_with: deserialize_with.get(),
            bound: bound.get(),
            nested: nested.get(),
            flatten: flatten.get(),
        }
    }

    pub fn name(&self) -> &Name {
        &self.name
    }

    pub fn rename_by_rules(&mut self, rules: &RenameRule) {
        if !self.name.renamed {
            self.name.name = rules.apply_to_field(&self.name.name);
        }
    }

    pub fn flatten(&self) -> bool {
        self.flatten
    }

    pub fn nested(&self) -> bool {
        self.nested
    }

    pub fn skip_serializing(&self) -> bool {
        self.skip_serializing
    }

    pub fn skip_deserializing(&self) -> bool {
        self.skip_deserializing
    }

    pub fn default(&self) -> &Default {
        &self.default
    }

    pub fn serialize_with(&self) -> Option<&syn::ExprPath> {
        self.serialize_with.as_ref()
    }

    pub fn deserialize_with(&self) -> Option<&syn::ExprPath> {
        self.deserialize_with.as_ref()
    }

    pub fn bound(&self) -> Option<&[syn::WherePredicate]> {
        self.bound.as_ref().map(|vec| &vec[..])
    }
}

pub fn get_klickhouse_meta_items(cx: &Ctxt, attr: &syn::Attribute) -> Result<Vec<syn::Meta>, ()> {
    if !attr.path().is_ident(&KLICKHOUSE) {
        return Ok(Vec::new());
    }

    let nested = match attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) {
        Ok(nested) => nested,
        Err(err) => {
            cx.syn_error(err);

            return Err(());
        }
    };

    Ok(nested.into_iter().collect())

    // let mut result = Vec::new();

    // for meta in nested {
    //     match meta {
    //         List(list) => {
    //             result.push(list);
    //         },
    //         other => {
    //             cx.error_spanned_by(other, "expected #[klickhouse(...)]");

    //             return Err(())
    //         }
    //     }
    // }

    // Ok(result)
}

fn get_lit_str<'a>(cx: &Ctxt, attr_name: Symbol, lit: &'a syn::Lit) -> Result<&'a syn::LitStr, ()> {
    get_lit_str2(cx, attr_name, attr_name, lit)
}

fn get_lit_str2<'a>(
    cx: &Ctxt,
    attr_name: Symbol,
    meta_item_name: Symbol,
    lit: &'a syn::Lit,
) -> Result<&'a syn::LitStr, ()> {
    if let syn::Lit::Str(lit) = lit {
        Ok(lit)
    } else {
        cx.error_spanned_by(
            lit,
            format!(
                "expected klickhouse {} attribute to be a string: `{} = \"...\"`",
                attr_name, meta_item_name
            ),
        );
        Err(())
    }
}

fn parse_lit_into_expr_path(
    cx: &Ctxt,
    attr_name: Symbol,
    lit: &syn::Lit,
) -> Result<syn::ExprPath, ()> {
    let string = get_lit_str(cx, attr_name, lit)?;
    parse_lit_str(string).map_err(|_| {
        cx.error_spanned_by(lit, format!("failed to parse path: {:?}", string.value()))
    })
}

fn parse_lit_into_where(
    cx: &Ctxt,
    attr_name: Symbol,
    meta_item_name: Symbol,
    lit: &syn::Lit,
) -> Result<Vec<syn::WherePredicate>, ()> {
    let string = get_lit_str2(cx, attr_name, meta_item_name, lit)?;
    if string.value().is_empty() {
        return Ok(Vec::new());
    }

    let where_string = syn::LitStr::new(&format!("where {}", string.value()), string.span());

    parse_lit_str::<syn::WhereClause>(&where_string)
        .map(|wh| wh.predicates.into_iter().collect())
        .map_err(|err| cx.error_spanned_by(lit, err))
}

fn parse_lit_into_ty(cx: &Ctxt, attr_name: Symbol, lit: &syn::Lit) -> Result<syn::Type, ()> {
    let string = get_lit_str(cx, attr_name, lit)?;

    parse_lit_str(string).map_err(|_| {
        cx.error_spanned_by(
            lit,
            format!("failed to parse type: {} = {:?}", attr_name, string.value()),
        )
    })
}

fn parse_lit_str<T>(s: &syn::LitStr) -> parse::Result<T>
where
    T: Parse,
{
    let tokens = spanned_tokens(s)?;
    syn::parse2(tokens)
}

fn spanned_tokens(s: &syn::LitStr) -> parse::Result<TokenStream> {
    let stream = syn::parse_str(&s.value())?;
    Ok(respan(stream, s.span()))
}
