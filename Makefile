# Run cargo fmt

fmt:
	cd server/src/ && cargo fmt

# Run server
run:
	cd server && cargo run

# Helm template
template:
	helm template parseable \
	helm/parseable \
	-f helm/parseable/values.yaml

# Helm Upgrade
upgrade:
	helm upgrade --install \
	parseable --namespace parseable \
	--create-namespace \
	helm/parseable \
	-f helm/parseable/values.yaml
