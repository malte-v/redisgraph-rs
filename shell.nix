{ rustDate ? "2020-11-01" }:

let
  mozillaOverlay = import (builtins.fetchTarball "https://github.com/mozilla/nixpkgs-mozilla/archive/8c007b60731c07dd7a052cce508de3bb1ae849b4.tar.gz");
  pkgs = import <nixpkgs> {
    overlays = [ mozillaOverlay ];
  };
  rustChannel = pkgs.rustChannelOf { date = rustDate; channel = "nightly"; };
in pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    rustChannel.rust
    rustfmt
  ];
}
