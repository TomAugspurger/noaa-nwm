.PHONY: dask-operator

setup:
	helm repo add dask https://helm.dask.org
	helm repo update


dask-operator:
	helm upgrade --install dask-operator dask/dask-kubernetes-operator \
		--wait
