from __future__ import annotations

import os


DEFAULT_BASE_URL = "http://localhost:1234/v1"
DEFAULT_MODEL = "google/gemma-4-e4b"


def main() -> None:
    base_url = os.getenv("OPENAI_BASE_URL", DEFAULT_BASE_URL)
    api_key = os.getenv("OPENAI_API_KEY") or ("lm-studio" if "localhost" in base_url else None)
    model = os.getenv("OPENAI_MODEL", DEFAULT_MODEL)

    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for non-local OpenAI-compatible endpoints")

    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError("openai package is not installed. Run: python -m pip install -r ML_prediction_price/requirements.txt") from exc

    client = OpenAI(base_url=base_url, api_key=api_key, timeout=60)

    models = client.models.list()
    model_ids = [item.id for item in models.data]
    print(f"Base URL: {base_url}")
    print(f"Models: {model_ids}")
    if model not in model_ids:
        raise RuntimeError(f"Expected model {model!r} was not returned by LM Studio")

    response = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": "Return only a compact JSON object.",
            },
            {
                "role": "user",
                "content": 'Extract car JSON from: Toyota Camry 2021 3.5. Use keys brand, model, year, engine_volume_l.',
            },
        ],
    )
    content = response.choices[0].message.content
    print("Response:")
    print(content)


if __name__ == "__main__":
    main()
