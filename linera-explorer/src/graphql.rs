use serde_json::Value;

pub async fn introspection(url: &str) -> Result<Value, String> {
    let client = reqwest::Client::new();
    let res = client.post(url).body("{\"query\":\"query {__schema{queryType{name}mutationType{name}subscriptionType{name}types{...FullType}directives{name description locations args{...InputValue}}}}fragment FullType on __Type{kind name description fields(includeDeprecated:true){name description args{...InputValue}type{...TypeRef}isDeprecated deprecationReason}inputFields{...InputValue}interfaces{...TypeRef}enumValues(includeDeprecated:true){name description isDeprecated deprecationReason}possibleTypes{...TypeRef}}fragment InputValue on __InputValue{name description type{...TypeRef}defaultValue}fragment TypeRef on __Type{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name ofType{kind name}}}}}}}}\"}").send().await.map_err(|e| e.to_string())?.text().await.map_err(|e| e.to_string())?;
    serde_json::from_str::<Value>(&res).map_err(|e| e.to_string())
}
