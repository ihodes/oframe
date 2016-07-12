.PHONY: all clean

all:
	ocamlbuild -package compiler-libs.common -package nonstd -package csv  df_1.native

clean:
	ocamlbuild -clean
