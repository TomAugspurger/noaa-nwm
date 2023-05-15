accet = xr.open_dataset(
    fs.open(f"{prefix}/short_range/nwm.t00z.short_range.land.f018.conus.nc")
)["ACCET"].load()

fig, ax = plt.subplots(figsize=(16, 10))
accet.coarsen(x=4, y=4, boundary="trim").mean().plot(ax=ax)
