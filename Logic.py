import asyncio

from DataBaseMananger import OldDataBaseManager, NewDataBaseManager
from AIManager import AIManager


class Logic:
    def __init__(self):
        self.old_dbm = OldDataBaseManager('todo')
        self.new_dbm = NewDataBaseManager('todo')
        self.ai_manager = AIManager()

    async def Build(self):
        # создаем таблицы и соединение
        old_dbm_build = asyncio.create_task(self.old_dbm.Build())
        new_dbm_build = asyncio.create_task(self.new_dbm.Build())
        ai_connect = asyncio.create_task(self.ai_manager.Connect())

        await old_dbm_build
        await new_dbm_build
        await ai_connect

    '''
    input: ['width', 'height']
    output: {'width': '10', 'height': '42'}
    '''
    async def GetPropValues(self, properties: list[str]) -> dict:
        dictionary = dict()
        for prop in properties:
            dictionary[prop] = self.ai_manager.GetPropValue(prop)
        return dictionary

    async def FillRowGroup(self, group_name, note, properties):
        dictionary = dict()
        dictionary['group_name'] = group_name
        dictionary['okpd2_parent'] = note.OKPD2
        for i, prop in enumerate(properties):
            dictionary[f"property{i}"] = prop
        return dictionary

    # todo attention
    async def FillRowMTR(self, group_id, properties_values, note) -> dict:
        dictionary = dict()
        dictionary['skmtr_code'] = note.SKMTR
        dictionary['name'] = note.Name
        dictionary['marking'] = note.Marking
        dictionary['requlations'] = note.Regulations
        dictionary['parametrs'] = note.Parametrs
        dictionary['base_unit'] = note.Base_unit
        dictionary['okpd2'] = note.OKPD2
        dictionary['group_id'] = group_id
        for i, prop in enumerate(properties_values):
            dictionary[f'property{i}'] = prop
        return dictionary

    async def ChooseGroup(self, note):
        # для заданного ОКПД2 выбираем группу
        various_groups: dict = await self.new_dbm.GetChildrensByOKPD2(note.OKPD2) # получаем словарь (имяГруппы : Значение)
        recomended_group: str = await self.ai_manager.ChooseGroup(
            note.Name,
            list(various_groups.keys())
        ) # Нейронка предлагает группу
        if recomended_group not in various_groups.keys(): # предложили свой вариант
            # получаем свойства от нейронки
            properties = await self.ai_manager.GetProperties(recomended_group)

            # заносим в БД
            await self.new_dbm.InsertRowInGroups(
                # заполняем свойства
                self.FillRowGroup(recomended_group, note, properties)
            ) # записали новую группу

            # ОБНОВЛЯЕМ варианты групп
            various_groups: dict = await self.new_dbm.GetChildrensByOKPD2(note.OKPD2)
            # получаем словарь (имяГруппы : Значение)
        else:
            # Получаем существующие Properties
            properties = await self.new_dbm.GetPropertiesByGroupID(various_groups[recomended_group])

        # Находим заначения заданных свойств
        prop_values:dict = self.GetPropValues(note.Name, properties)

        # записываем их
        await self.new_dbm.InsertRowMTR(
            # формируем словарь
            await self.FillRowMTR(various_groups[recomended_group], prop_values, note)
        )

    async def InsertNew(self, note):
        # выбираем походящий класс
        roots = await self.new_dbm.GetRoots() # вернуть словарь формата: {описание_Класса : okpd[из 2 символов]}
        recomended_class = await self.ai_manager.ChooseFromOld(roots, note.Name)
        cur_okpd2 = roots[recomended_class] # получаем число зарактеризующее класс XX

        # Выбираем подходящий подкласс (начинается как okpd2_class класс и содержит 4 символа)
        # Возращает словарь формата: {описание подКласса: его OKPD из n символов}
        sub_classes = await self.new_dbm.GetSubClasses(cur_okpd2, 4)
        recomended_sub_class = await self.ai_manager.ChooseFromOld(sub_classes, note.Name)
        cur_okpd2 = sub_classes[recomended_sub_class]

        groupes = await self.new_dbm.GetSubClasses(cur_okpd2, 5)
        recomended_group = await self.ai_manager.ChooseFromOld(groupes, note.Name)
        cur_okpd2 = groupes[recomended_group]

        sub_groupes = await self.new_dbm.GetSubClasses(cur_okpd2, 7)
        recomended_sub_group = await self.ai_manager.ChooseFromOld(sub_groupes, note.Name)
        cur_okpd2 = sub_groupes[recomended_sub_group]

        kind = await self.new_dbm.GetSubClasses(cur_okpd2, 8)
        recomended_kind = await self.ai_manager.ChooseFromOld(kind, note.Name)
        cur_okpd2 = kind[recomended_kind]

        category = await self.new_dbm.GetSubClasses(cur_okpd2, 12)
        recomended_category = await self.ai_manager.ChooseFromOld(category, note.Name)
        cur_okpd2 = category[recomended_category]

        # todo
        # ub_category = await self.new_dbm.GetSubClasses(cur_okpd2, 7)
        # recomended_sub_category = await self.ai_manager.ChooseFromOld(sub_category, note.Name)
        # cur_okpd2 = sub_category[recomended_sub_category]

        note.OKPD2 = cur_okpd2
        await self.ChooseGroup(note)

    async def ParseOldInNew(self):
        for note in await self.old_dbm.GetMTR():
            answer = self.IsNotWrong(note) # верно ли определен класс?
            if answer != "": # нейронка нашла противоречие
                print(f"Найдено противоречие: {answer}")
                print("Продолжить как есть или оставить выбор ИИ")

                inp = input("Доверится ИИ? [y(DEFAULT) / n]: ")

                ## Стоять на своем
                if inp == "n":
                    await self.ChooseGroup(note)
                ## Автоисправление
                else:
                    await self.InsertNew(note)


    async def levels(self, okpd):
        return [okpd[:2], okpd[:4], okpd[:5], okpd[:7], okpd[:8], okpd[:9]]

    async def IsNotWrong(self, note) -> str:
        # okpd_level - циферки из [XX, XX.X, XX.XX, ...]
        for okpd_level in await self.levels(note.OKPD2):
            str_level = await self.new_dbm.GetValueByOKPD2(note.OKPD2) # строчное представление OKPD
            answer_ai = await self.ai_manager.IsValid(note.Name, str_level) # подходит ли выбранный класс под нашу запись
            if answer_ai != "":
                return answer_ai
        return ""
