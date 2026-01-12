module DEMOSJsonReader

using DEMOSObjects: DEMOSObjects
using EzXML: EzXML, eachelement, readxml
using JSON: JSON

include("parse.jl")

include("distributions/parse.jl")
include("functions/parse.jl")
include("likelihoods/parse.jl")

end
