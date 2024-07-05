{
  description = "Rust dev environment";

  # Flake inputs
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs"; # also valid: "nixpkgs"
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {  
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          buildInputs = with pkgs; 
            [ rust-analyzer
              hurl
              (rust-bin.stable.latest.default.override {
                extensions = ["rust-src"];
              })
            ] ++ 
              (if system == "aarch64-darwin" then [ darwin.apple_sdk.frameworks.Security ] 
              else []);
        in
        with pkgs;
        {
          devShells.default = mkShell {
            inherit buildInputs;
            RUST_SRC_PATH="${rust.packages.stable.rustPlatform.rustLibSrc}";
          };
        }
      ); 
}
