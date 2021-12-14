### A Pluto.jl notebook ###
# v0.14.5

using Markdown
using InteractiveUtils

# ╔═╡ 6e4ef6e8-af5f-11eb-0aab-1d5b28db3ddd
using Hashpipe, Blio, PlutoUI, Statistics, Plots

# ╔═╡ 4997df76-68a5-468b-9b47-ecd065a84a08
using HPGuppi

# ╔═╡ d6768b00-a5e6-494a-b5ac-a5bd3fc3c1ab
begin
	status = Hashpipe.status_t(0,0,0,0)
	Hashpipe.status_attach(0, Ref(status))
	db_p = Hashpipe.databuf_attach(0, 1)
	db = HPGuppi.hpguppi_databuf_t(db_p)
end

# ╔═╡ e9d43e26-e4e7-4512-a8be-48b56ebf2778
with_terminal() do
	display(status)
end

# ╔═╡ ea6771e2-386c-4fe8-9f55-f3bfadb14d64
with_terminal() do
	display(db.p_hp_db)
end

# ╔═╡ c605c242-4b64-4e33-950d-2694d4e798c3
grh, raw_data = HPGuppi.hpguppi_get_hdr_block(db.blocks[1]);

# ╔═╡ 040dd3a4-0963-4bf9-8791-c60f3d540eef
grh

# ╔═╡ 09ec01ce-6e82-41bb-a3ad-a06b7ccffa6e
power = abs2.(Complex{Int16}.(raw_data))

# ╔═╡ f986999e-26a9-4d8b-be74-f173c662e03a
pol_bandpass = mean(power, dims=(2))

# ╔═╡ b2b540e3-5564-4f90-9808-db7ef87ac46a
begin
	p = plot(pol_bandpass[1,1,:]);
	plot!(p,pol_bandpass[2,1,:])
end

# ╔═╡ Cell order:
# ╠═6e4ef6e8-af5f-11eb-0aab-1d5b28db3ddd
# ╠═4997df76-68a5-468b-9b47-ecd065a84a08
# ╠═d6768b00-a5e6-494a-b5ac-a5bd3fc3c1ab
# ╠═e9d43e26-e4e7-4512-a8be-48b56ebf2778
# ╠═ea6771e2-386c-4fe8-9f55-f3bfadb14d64
# ╠═c605c242-4b64-4e33-950d-2694d4e798c3
# ╠═040dd3a4-0963-4bf9-8791-c60f3d540eef
# ╠═09ec01ce-6e82-41bb-a3ad-a06b7ccffa6e
# ╠═f986999e-26a9-4d8b-be74-f173c662e03a
# ╠═b2b540e3-5564-4f90-9808-db7ef87ac46a
