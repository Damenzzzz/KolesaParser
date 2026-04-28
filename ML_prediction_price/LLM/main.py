
import json
import os
from typing import Any
import re
from langchain.tools import tool
import openai
from langchain_openai import ChatOpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY


def _openai_client() -> openai.OpenAI:
    return openai.OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else openai.OpenAI(api_key=None)

import pickle
import pandas as pd
import numpy as np
from catboost import CatBoostRegressor
from langchain.agents import create_agent
from dotenv import load_dotenv
from proccessing import clean_base, make_X

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

cat_model = CatBoostRegressor()
cat_model.load_model('/Users/nurdauletaldibek/Desktop/FINAL_project/ML_for_predict/car_price_catboost.cbm')

with open('/Users/nurdauletaldibek/Desktop/FINAL_project/ML_for_predict/car_price_preprocess.pkl', "rb") as f:
    meta = pickle.load(f)

features = meta['features']
category_maps = meta['category_maps']
train_mileage_median = meta['train_mileage_median']


def predict_price(car_info: dict) -> float:
    car_df = pd.DataFrame([car_info])
    car_clean = clean_base(car_df, is_train=False)
    X = make_X(car_clean,category_maps=category_maps, features=features, train_mileage_median=train_mileage_median)
    price = np.expm1(cat_model.predict(X)[0])
    return price



def extract_json_from_text(text: str) -> dict:
    """
    Достаёт JSON из ответа LLM.
    Даже если модель случайно добавила текст вокруг JSON.
    """
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"LLM did not return JSON: {text}")

    return json.loads(match.group(0))

def extract_car_info(user_text: str) -> dict:
    system_prompt = """
    Ты превращаешь текст пользователя в JSON с характеристиками автомобиля.

    Верни ТОЛЬКО JSON. Без markdown, без объяснений.

    Поля:
    brand, model, year, mileage_km, engine_volume_l, fuel_type,
    transmission, drive_type, steering_wheel, color, generation.

    Правила:
    - Если бренд написан по-русски, переведи в английское название: Тойота -> Toyota, БМВ -> BMW.
    - Если модель Камри -> Camry.
    - fuel_type используй: бензин, дизель, гибрид, электро, газ.
    - transmission используй: Автомат, Механика, Вариатор, Робот.
    - drive_type используй: Передний привод, Задний привод, Полный привод.
    - steering_wheel используй: Слева или Справа.
    - Если поле неизвестно, ставь null.
    - generation если неизвестно, ставь "unknown".

    Пример:
    {
    "brand": "Toyota",
    "model": "Camry",
    "year": 2021,
    "mileage_km": 80000,
    "engine_volume_l": 2.5,
    "fuel_type": "бензин",
    "transmission": "Автомат",
    "drive_type": "Передний привод",
    "steering_wheel": "Слева",
    "color": "Серый",
    "generation": "XV70"
    }
    """

    response = llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text}
    ])

    content = response.content
    car_info = extract_json_from_text(content)

    return car_info

@tool
def ML_predict(car_info_json: str) -> str:
    """
    Предсказывает цену автомобиля.

    На вход принимает JSON string с характеристиками машины:
    brand, model, year, mileage_km, engine_volume_l, fuel_type,
    transmission, drive_type, steering_wheel, color, generation.
    """
    car_info = json.loads(car_info_json)

    price = predict_price(car_info)

    low = price * 0.85
    high = price * 1.15

    return (
        f"Predicted price: {price:,.0f} ₸\n"
        f"Estimated range: {low:,.0f} - {high:,.0f} ₸"
    )

llm = ChatOpenAI(
    base_url="http://localhost:1234/v1",
    api_key="lm-studio",
    model="google/gemma-4-e4b",
    temperature=0
)

agent = create_agent(
    model=llm,
    tools=[ML_predict ],
    system_prompt="Ты авто-ассистент. Если нужно предсказать цену машины, используй ML_predict.Но сам не придумывай цену, а всегда вызывай ML_predict с правильными характеристиками машины. Если не хватает данных, попроси их у пользователя."
)
if __name__ == "__main__":
    # Пример использования агента
    car_info = {
        "brand": "Toyota",
        "model": "Camry",
        "year": 2022,
        "mileage_km": 55900,
        "engine_volume_l": 2.5,
        "fuel_type": "бензин",
        "transmission": "автомат",
        "drive_type": "Передний привод",
        "steering_wheel": "Слева",
        "color": "черный",
        "generation": "V75"
    }
    response = agent.invoke(
        {'messages':[{'role':'user','content':"Камри 2022 года, 55900 км, 2.5 литра, бензин, автомат, передний привод, руль слева, черная, поколение V75. выведи json?"}]}
       # {"messages":[{'role':'user','content':f"Сколько стоит машина с такими характеристиками? {json.dumps(car_info)}"}]}
    )
    print(response["messages"][-1].content)