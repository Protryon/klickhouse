use proc_macro2::TokenStream;

pub fn wrap_in_const(code: TokenStream) -> TokenStream {
    quote! {
        #[doc(hidden)]
        #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
        const _: () = {
            #code
        };
    }
}
