#[macro_use]
extern crate syn;
#[macro_use]
extern crate quote;


mod row;
mod fragment;
mod ast;
mod attr;
mod symbol;
mod case;
mod respan;
mod ctxt;
mod internal;
mod check;
mod receiver;
mod bound;
mod dummy;

use proc_macro::TokenStream;
use syn::DeriveInput;

fn to_compile_errors(errors: Vec<syn::Error>) -> proc_macro2::TokenStream {
    let compile_errors = errors.iter().map(syn::Error::to_compile_error);
    quote!(#(#compile_errors)*)
}

#[proc_macro_derive(Row, attributes(klickhouse))]
pub fn derive_serialize(input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    row::expand_derive_serialize(&mut input)
        .unwrap_or_else(to_compile_errors)
        .into()
}
