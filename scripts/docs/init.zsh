if [[ "$(uname)" == "Darwin" ]]
then export DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib # for macOS (https://squidfunk.github.io/mkdocs-material/plugins/requirements/image-processing/#troubleshooting)
fi
