from typing import List

class TemplateArguments:
    def __init__(self, key: str, value: str) -> None:
        self.key = key
        self.value = value

class EmailTemplate:
    def __init__(self, template_name: str = None, arguments: List[TemplateArguments] = []) -> None:
        self.template_name = template_name
        self.template_path = f"templates/email/{template_name}.html"
        self.arguments = arguments

    def __read_template(self) -> str:
        with open(self.template_path, "r") as file:
            return file.read()

    def __replace_arguments(self, template: str) -> str:
        for argument in self.arguments:
            template = template.replace(f"{{{{ {argument.key} }}}}", argument.value)
        return template

    def render(self) -> str:
        return self.__replace_arguments(self.__read_template())

