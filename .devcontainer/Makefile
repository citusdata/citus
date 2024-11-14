
init: ../.vscode/c_cpp_properties.json ../.vscode/launch.json

../.vscode:
	mkdir -p ../.vscode

../.vscode/launch.json: ../.vscode .vscode/launch.json
	cp .vscode/launch.json ../.vscode/launch.json

../.vscode/c_cpp_properties.json: ../.vscode
	./.vscode/generate_c_cpp_properties-json.py ../.vscode/c_cpp_properties.json
