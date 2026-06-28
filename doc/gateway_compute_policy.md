# Minimal Compute Policy para Lakeflow Gateway

Esta doc registra como criar a compute policy usada pelo Lakeflow ingestion gateway e como atualizar o pipeline do gateway via Databricks CLI.

## Objetivo

Criar uma policy pequena e controlada para o gateway CDC do Lakeflow, fixando:

- `num_workers = 1`
- `driver_node_type_id = Standard_E4d_v4`
- `node_type_id = Standard_F4s`

Policy atual usada no projeto:

```text
0005F147C3FA0AA4
```

Gateway atual do projeto:

```text
gw_restaurant_cdc_ingestion
```

Pipeline ID atual do gateway:

```text
92e04cbb-1b29-4a6d-a3f4-ce27f4fc5f34
```

## Criar Pela UI

1. Acesse `Compute > Policies`.
2. Clique em `Create policy`.
3. Nomeie como:

```text
Minimal Compute Policy
```

4. Abra a aba `Definitions`.
5. Selecione o modo `JSON`.
6. Cole a policy abaixo:

```json
{
  "num_workers": {
    "type": "fixed",
    "value": 1
  },
  "driver_node_type_id": {
    "type": "fixed",
    "value": "Standard_E4d_v4"
  },
  "node_type_id": {
    "type": "fixed",
    "value": "Standard_F4s"
  }
}
```

7. Salve a policy.
8. Copie o `policy_id` gerado. No ambiente atual, o ID usado foi:

```text
0005F147C3FA0AA4
```

## Criar Pela CLI

Opcionalmente, crie a mesma policy pela CLI:

```bash
databricks cluster-policies create --json '{
  "name": "Minimal Compute Policy",
  "description": "Minimal Compute Policy",
  "definition": "{\"num_workers\":{\"type\":\"fixed\",\"value\":1},\"driver_node_type_id\":{\"type\":\"fixed\",\"value\":\"Standard_E4d_v4\"},\"node_type_id\":{\"type\":\"fixed\",\"value\":\"Standard_F4s\"}}"
}'
```

Para listar policies e confirmar o ID:

```bash
databricks cluster-policies list -o json
```

## Atualizar o Gateway Pela CLI

Use este comando para atualizar o pipeline do gateway e aplicar a policy:

```bash
databricks pipelines update 92e04cbb-1b29-4a6d-a3f4-ce27f4fc5f34 --json '{
  "name": "gw_restaurant_cdc_ingestion",
  "catalog": "restaurant_lakehouse_dev",
  "schema": "00_landing",
  "continuous": true,
  "gateway_definition": {
    "connection_name": "cnn_restaurantops",
    "gateway_storage_catalog": "restaurant_lakehouse_dev",
    "gateway_storage_schema": "00_landing",
    "gateway_storage_name": "gw_restaurant_cdc_ingestion"
  },
  "clusters": [
    {
      "label": "default",
      "policy_id": "0005F147C3FA0AA4",
      "apply_policy_default_values": true
    }
  ]
}'
```

## Alternativa Com Arquivo JSON

Crie um arquivo local chamado `gateway_update.json` com:

```json
{
  "name": "gw_restaurant_cdc_ingestion",
  "catalog": "restaurant_lakehouse_dev",
  "schema": "00_landing",
  "continuous": true,
  "gateway_definition": {
    "connection_name": "cnn_restaurantops",
    "gateway_storage_catalog": "restaurant_lakehouse_dev",
    "gateway_storage_schema": "00_landing",
    "gateway_storage_name": "gw_restaurant_cdc_ingestion"
  },
  "clusters": [
    {
      "label": "default",
      "policy_id": "0005F147C3FA0AA4",
      "apply_policy_default_values": true
    }
  ]
}
```

Depois rode:

```bash
databricks pipelines update 92e04cbb-1b29-4a6d-a3f4-ce27f4fc5f34 --json @gateway_update.json
```

## Como Descobrir o Pipeline ID do Gateway

Se o ID mudar, liste os pipelines:

```bash
databricks pipelines list-pipelines -o json
```

Depois filtre pelo nome do gateway:

```bash
databricks pipelines list-pipelines --filter "name = 'gw_restaurant_cdc_ingestion'" -o json
```

Use o `pipeline_id` retornado no comando `databricks pipelines update`.

## Asset Bundle

No bundle, a mesma policy fica declarada em:

```yaml
resources:
  pipelines:
    pipeline_gw_ingestion_restaurantops:
      name: gw_restaurant_cdc_ingestion
      clusters:
        - label: default
          policy_id: 0005F147C3FA0AA4
          apply_policy_default_values: true
```

Quando possivel, prefira manter essa configuracao no Asset Bundle para evitar drift entre UI, CLI e codigo.
