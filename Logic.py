import asyncio
import logging
from DataBaseMananger import OldDataBaseManager, NewDataBaseManager
from AIManager import AIManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

db_config = {
    'db': 'oldbase',
    'user': 'root',
    'password': 'password!',
    'host': 'localhost',
    'port': 3306
}
db_new = {
    'db': 'newbase1',
    'user': 'root',
    'password': 'password!',
    'host': 'localhost',
    'port': 3306
}
# Path to Excel file
excel_path = 'MTR.xlsx'

class Logic:
    def __init__(self):
        self.old_dbm = OldDataBaseManager(db_config, excel_path)
        self.new_dbm = NewDataBaseManager(db_new)
        self.ai_manager = AIManager()

    async def Build(self):
        logging.info("Starting the build process for both databases and AI connection.")
        try:
            # create tables and connection
            old_dbm_build = asyncio.create_task(self.old_dbm.Build())
            await old_dbm_build
            new_dbm_build = asyncio.create_task(self.new_dbm.Build())
            await new_dbm_build
            ai_connect = asyncio.create_task(self.ai_manager.Connect())
            await ai_connect
            logging.info("Build process completed.")
        except Exception as e:
            logging.error(f"Error during build process: {e}")

    async def GetPropValues(self,name, properties: list[str]) -> dict:
        logging.info(f"Getting property values for: {properties}")
        dictionary = dict()

        try:
            for prop in properties:
                dictionary[prop] = await self.ai_manager.GetPropValue(prop,name)
        except Exception as e:
            logging.error(f"Error while getting property values: {e}")
        return dictionary

    async def FillRowGroup(self, group_name, note, properties):
        logging.info(f"Filling group row for group: {group_name}")
        dictionary = dict()
        dictionary['group_name'] = group_name
        dictionary['okpd2_parent'] = note.OKPD2
        for i, prop in enumerate(properties):
            dictionary[f"property{i}"] = prop
        return dictionary

    async def FillRowMTR(self, group_id, properties_values, note) -> dict:
        logging.info(f"Filling MTR row for SKMTR code: {note.SKMTR}")
        dictionary = dict()
        try:
            dictionary['SKMTR'] = note.SKMTR
            dictionary['Name'] = note.Name
            dictionary['Marking'] = note.Marking
            dictionary['Requlations'] = note.Regulations
            dictionary['Parametrs'] = note.Parametrs
            dictionary['Base_unit'] = note.Base_unit
            dictionary['OKPD2'] = note.OKPD2
            dictionary['group_id'] = group_id
            for i, prop in enumerate(properties_values):
                dictionary[f'property{i}'] = prop
        except Exception as e:
            logging.error(f"Error while filling MTR row: {e}")
        return dictionary

    async def ChooseGroup(self, note):
        logging.info(f"Choosing group for note: {note.OKPD2}")
        try:
            various_groups: dict = await self.new_dbm.GetChildrensByOKPD2(note.OKPD2)
            recommended_group: str = await self.ai_manager.ChooseGroup(note.Name, list(various_groups.keys()))

            if recommended_group not in various_groups.keys():
                logging.info(f"New group recommended by AI: {recommended_group}")
                properties = await self.ai_manager.GetProperties(recommended_group)
                await self.new_dbm.InsertRowInGroups(await self.FillRowGroup(recommended_group, note, properties))
                various_groups: dict = await self.new_dbm.GetChildrensByOKPD2(note.OKPD2)
            else:
                properties = await self.new_dbm.GetPropertiesByGroupID(various_groups[recommended_group])

            prop_values: dict = await self.GetPropValues(properties,note.Name)
            await self.new_dbm.InsertRowMTR(await self.FillRowMTR(various_groups[recommended_group], prop_values, note))


            logging.info(f"Group selected and row inserted for note: {note.OKPD2}")
        except Exception as e:
            logging.error(f"Error while choosing group: {e}")

    async def NotStrictEquality(self,rec_obj,dictionary:dict):
        for key,value in dictionary.items():
            if key in rec_obj:
                return value
        logging.warning('Не найдено ни одного совпадения с вариантом ИИ.')
        return dictionary[list(dictionary.keys())[0]]

    async def InsertNew(self, note):
        logging.info(f"Inserting new note: {note.SKMTR}")
        try:
            roots = await self.new_dbm.GetRoots()
            recommended_class = await self.ai_manager.ChooseFromOld(roots.keys(), note.Name)
            cur_okpd2 = await self.NotStrictEquality(recommended_class, roots)

            # Iteratively select the subclass, group, subgroup, etc.
            sub_classes = await self.new_dbm.GetSubClasses(cur_okpd2, 4)
            recommended_sub_class = await self.ai_manager.ChooseFromOld(list(sub_classes.keys()), note.Name)
            cur_okpd2 = await self.NotStrictEquality(recommended_sub_class, sub_classes)

            groups = await self.new_dbm.GetSubClasses(cur_okpd2, 5)
            recommended_group = await self.ai_manager.ChooseFromOld(list(groups.keys()), note.Name)
            print('Рекомендованная группа',recommended_group)
            print(groups)
            cur_okpd2 = await self.NotStrictEquality(recommended_group, groups)
            print('cur_okpd2',cur_okpd2)

            sub_groups = await self.new_dbm.GetSubClasses(cur_okpd2, 7)
            recommended_sub_group = await self.ai_manager.ChooseFromOld(list(sub_groups.keys()), note.Name)
            cur_okpd2 = await self.NotStrictEquality(recommended_sub_group, sub_groups)
            print('Рекомендованная сабгруппа',recommended_sub_group)
            print('cur_okpd2',cur_okpd2)

            kind = await self.new_dbm.GetSubClasses(cur_okpd2, 8)
            recommended_kind = await self.ai_manager.ChooseFromOld(list(kind.keys()), note.Name)
            cur_okpd2 = await self.NotStrictEquality(recommended_kind, kind)
            print('Рекоманд кайнд',recommended_kind)
            print('cur_okpd2',cur_okpd2)

            category = await self.new_dbm.GetSubClasses(cur_okpd2, 12)
            recommended_category = await self.ai_manager.ChooseFromOld(list(category.keys()), note.Name)
            cur_okpd2 = await self.NotStrictEquality(recommended_category, category)
            print('Рекомендованная категория',recommended_category)
            print('cur_okpd2',cur_okpd2)

            note.OKPD2 = cur_okpd2
            print('Ноут',note)
            await self.ChooseGroup(note)
            logging.info(f"New note inserted with OKPD2: {cur_okpd2}")
        except Exception as e:
            logging.error(f"Error while inserting new note: {e}")

    async def ParseOldInNew(self):
        logging.info("Parsing old database into new format.")
        try:
            async for note in self.old_dbm.GetMTR():
                answer = await self.IsNotWrong(note)
                if answer != ".":
                    logging.warning(f"Inconsistency found: {answer}")
                    inp = input("Trust AI? [y(DEFAULT) / n]: ")
                    if inp == "n":
                        await self.ChooseGroup(note)
                    else:
                        await self.InsertNew(note)
        except Exception as e:
            logging.error(f"Error while parsing old database: {e}")
    async def levels(self, okpd):
        return [okpd[:2], okpd[:4], okpd[:5], okpd[:7], okpd[:8], okpd[:12]]

    async def IsNotWrong(self, note) -> str:
        try:
            for okpd_level in await self.levels(note.OKPD2):
                str_level = await self.new_dbm.GetValueByOKPD2(note.OKPD2)

                answer_ai = await self.ai_manager.IsValid(note.Name, str_level)
                if answer_ai != ".":
                    return answer_ai
        except Exception as e:
            logging.error(f"Error during validation: {e}")
        return ""


# Entry point for execution
async def main():
    logic = Logic()
    await logic.Build()
    await logic.ParseOldInNew()


# Run the main loop
if __name__ == "__main__":
    logging.info("Starting the program.")
    try:
        asyncio.run(main())
    except Exception as e:
        logging.critical(f"Program crashed: {e}")
