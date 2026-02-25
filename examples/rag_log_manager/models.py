import yaml
from smolagents import OpenAIServerModel


__all__ = ("get_model", "test_model_connection")


def get_api_key(key_name):
    with open("/sdf/home/j/jchiang/.ai_api_keys") as fobj:
        api_key = yaml.safe_load(fobj)[key_name]
    return api_key


def test_model_connection(model):
    try:
        response = model(messages=[{"role": "user",
                                    "content": "Say 'Connection Successful'"}])
        print(f"Success! Model says: {response.content}")
    except Exception as e:
        print(f"Connection Failed!")
        print(f"Error details: {e}")


def get_model(model_id, test_connection=False):
    if model_id == "gemini-2.5-flash":
        api_base = "https://generativelanguage.googleapis.com/v1beta/openai/"
    elif model_id in ("gpt-5", "claude-4-5-sonnet", "claude-4-sonnet",
                      "imagen-3.0-generate-002"):
        api_base = "https://aiapi-prod.stanford.edu/v1/"
    else:
        raise RuntimeError(f"Unknown model: {model_id}")
    api_key = get_api_key(model_id)
    model = OpenAIServerModel(
        model_id=model_id,
        api_base=api_base,
        api_key=api_key
    )

    if test_connection:
        test_model_connection(model)

    return model
