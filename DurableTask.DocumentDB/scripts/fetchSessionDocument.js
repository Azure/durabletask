function (instanceId) {
            
    var context = getContext();
    var response = context.getResponse();
    var collection = context.getCollection();

    var query = 'SELECT * FROM c WHERE c.id = "' + instanceId + '" AND c.documentType = "SessionDocument"'; 

    var accept = collection.queryDocuments(collection.getSelfLink(), query, {},
        function (err, documents, responseOptions) {
            if (err) throw new Error("Error" + err.message);

            if (documents.length == 0) {
                getContext().getResponse().setBody(null);
                return;
            }

            if (documents.length > 1) {
                throw "Potential data corruption; query for sessionDocument " + instanceId + " resulted in multiple documents";
            }   

            response.setBody(documents[0]);
        });

    if (!accept) throw "Unable get session document";
}