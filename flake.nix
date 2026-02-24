{
  description = "ml-playground development shell (Nix + UV + CLI tooling)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        python = pkgs.python313;
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            python
            uv
            direnv
            git
            gh
            act
            jq
            just
          ];

          shellHook = ''
            export UV_PYTHON="${python}/bin/python3.13"
            export UV_LINK_MODE=copy
            export UV_PROJECT_ENVIRONMENT=".venv"
            export PRE_COMMIT_HOME=".cache/pre-commit"
            export RUFF_CACHE_DIR=".cache/ruff"
            export UV_CACHE_DIR=".cache/uv"
            export HYPOTHESIS_DATABASE_DIRECTORY=".cache/hypothesis"

            echo "[nix] dev shell ready (python: $(${python}/bin/python3.13 --version 2>&1))"
            echo "[nix] run: uv sync --group all"
          '';
        };
      }
    );
}
