import google.generativeai as genai

class AIManager:
    def __init__(self):
        self.model = '''NoIdentify 4 to 10 different numerical characteristics of the object: *. 
The characteristics must fit the object exactly and describe it exactly. If a characteristic does not fit, do not write it. It is not necessary to do all 10 characteristics.
    Print the result separated by commas.  The answer should contain nothing but a comma separated list of characteristicsne'''

    async def Connect(self):
        # Настройка и подключение к модели
        genai.configure(api_key="AIzaSyDLBgaKILxXEkfptkXRGGNWUo8kozPAnI4")
        self.model = genai.GenerativeModel(model_name="gemini-1.5-flash")

    async def ChooseGroup(self, name: str, group_names: list) -> str:
        # Формируем запрос для выбора группы
        prompt = f"To which group is the object best categorized: {name}. Here's a list of groups: {group_names} . Give the answer to the strictly selected group. Your answer must not contain anything other than the selected group. If none of the variants are not match to this object, then give me your best description."
        response = self.model.generate_content(prompt)
        return response.text.strip()

    async def ChooseFromOld(self, categories: list, name: str) -> str:
        # Формируем запрос для выбора старого класса
        prompt = f"To which group is the object best categorized: {name}. Here's a list of groups: {categories} . Give the answer to the strictly selected group. Your answer must not contain anything other than the selected group."
        response = self.model.generate_content(prompt)
        return response.text.strip()

    async def IsValid(self, name: str, class_desc: str) -> str:
        # Проверяем, подходит ли класс под запись
        prompt = f"Check if the object {name} corresponds to the group {class_desc}. The object will not match the group if there is a strong contradiction. Give the answer to the strictly answer. You can have two answers: a dot symbol (if the object belongs to a group), or a text explanation in Russian why the object does not belong to a group"
        response = self.model.generate_content(prompt)
        return response.text.strip()

    async def GetProperties(self, group_name: str) -> list[str]:
        # Получаем список свойств для группы
        prompt = f" Identify 4   to 10 different numerical characteristics of the object: {group_name}. The characteristics must fit the object exactly and describe it exactly. If a characteristic does not fit, do not write it. It is not necessary to do all 10 characteristics.Print the result separated by commas.  The answer should contain nothing but a comma separated list of characteristics in Russian"
        response = self.model.generate_content(prompt)
        return response.text.strip().split(", ")

    async def GetPropValue(self, name: str, property_name: list) -> dict:
        # Получаем значение свойства
        prompt = f"Here's the name of the facility: {name}. Here is its characteristic: {property_name}   . You have to find the exact numerical characteristic for this object.  Add unit of measurement to answer. The answer must strictly answer the question. Your answer must not contain anything  other than the selected group."
        response = self.model.generate_content(prompt)
        return response.text.strip()
