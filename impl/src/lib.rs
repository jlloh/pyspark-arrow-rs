use proc_macro::TokenStream;
use quote::quote;
use syn;

#[proc_macro_derive(HasArrowSparkSchema)]
pub fn macro_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_macro(&ast)
}

fn impl_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl HasArrowSparkSchema for #name {
            fn get_arrow_schema()  -> Vec<std::sync::Arc<arrow::datatypes::Field>> {
                pyspark_arrow_rs::get_arrow_schema::<Self>()
            }

            fn get_spark_ddl() -> anyhow::Result<String> {
                let fields = Self::get_arrow_schema();
                pyspark_arrow_rs::get_spark_ddl(fields)
            }
        }
    };
    gen.into()
}
