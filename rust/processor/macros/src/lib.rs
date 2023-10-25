use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(PGInsertable)]
pub fn insertable_trait(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);

    let struct_name = &ast.ident;

    // Generate code for implementing the trait
    let gen = match &ast.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let field_names = fields.named.iter().map(|field| &field.ident);
                let field_names_cloned = field_names.clone(); // Clone field names
                let insert_code = quote! {
                    impl PGInsertable for #struct_name {
                        fn get_insertable_sql_values(&self) -> (Vec<&str>, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) {
                            let mut column_names: Vec<&str> = vec![#(stringify!(#field_names_cloned)),*];
                            let mut query_values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![#(&self.#field_names as &(dyn tokio_postgres::types::ToSql + Sync)),*];

                            (column_names, query_values)
                        }
                    }
                };
                insert_code
            },
            _ => {
                quote! {
                    compile_error!("Only named fields are supported.");
                }
            },
        },
        _ => {
            quote! {
                compile_error!("Only structs are supported.");
            }
        },
    };

    gen.into()
}
