"""
Data Normalizer - Sistema de Normalização de Dados

Permite criar regras de normalização usando templates com placeholders.
Ideal para uso em APIs web (front + back).

Exemplo de uso:
    from app.services.data_normalizer import Normalizer, RuleBuilder

    normalizer = Normalizer()
    normalizer.add_rule(RuleBuilder.from_template(
        nome="cpf",
        template="{d3}.{d3}.{d3}-{d2}",
        descricao="CPF formatado"
    ))
    
    resultado = normalizer.normalize("12345678901", "cpf")
    # → "123.456.789-01"
"""

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any


# ============================================================
# MODELS
# ============================================================

@dataclass
class NormalizationRule:
    """
    Regra de normalização.
    
    Attributes:
        nome: Identificador único da regra
        descricao: Descrição legível da regra
        regex_entrada: Regex para extrair grupos da entrada
        template_saida: Template para formatar a saída
        pre_process: Pré-processamento a aplicar (ex: "digits_only")
        case_transforms: Transformações de case por grupo (ex: {"g1": "upper"})
    """
    nome: str
    descricao: str
    regex_entrada: str
    template_saida: str
    pre_process: Optional[str] = None
    case_transforms: Optional[Dict[str, str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário (para serialização JSON)."""
        return {
            "nome": self.nome,
            "descricao": self.descricao,
            "regex_entrada": self.regex_entrada,
            "template_saida": self.template_saida,
            "pre_process": self.pre_process,
            "case_transforms": self.case_transforms,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NormalizationRule":
        """Cria a partir de dicionário."""
        return cls(
            nome=data["nome"],
            descricao=data.get("descricao", ""),
            regex_entrada=data["regex_entrada"],
            template_saida=data["template_saida"],
            pre_process=data.get("pre_process"),
            case_transforms=data.get("case_transforms"),
        )


@dataclass
class NormalizationResult:
    """Resultado de uma normalização."""
    sucesso: bool
    valor_original: str
    valor_normalizado: Optional[str]
    regra_aplicada: Optional[str] = None
    erro: Optional[str] = None


# ============================================================
# RULE BUILDER
# ============================================================

class RuleBuilder:
    """
    Builder para criar regras de normalização.
    
    Sintaxe de Template:
        {d}     = 1 dígito
        {d3}    = 3 dígitos (número indica quantidade)
        {d?}    = 0 ou 1 dígito (OPCIONAL)
        {d3?}   = 0 ou 3 dígitos (OPCIONAL)
        {D}     = 1+ dígitos (quantidade variável)
        {D?}    = 0+ dígitos (opcional e variável)
        {l}     = 1 letra → minúscula
        {l3}    = 3 letras → minúsculas
        {l?}    = 0 ou 1 letra minúscula (OPCIONAL)
        {L}     = 1 letra → MAIÚSCULA
        {L3}    = 3 letras → MAIÚSCULAS
        {L?}    = 0 ou 1 letra maiúscula (OPCIONAL)
        {w}     = 1 alfanumérico (mantém case)
        {w3}    = 3 alfanuméricos
        {W}     = 1+ alfanuméricos (quantidade variável)
        
        Tudo fora das chaves é literal na saída.
    
    Exemplos:
        "{d3}.{d3}.{d3}-{d2}"    → CPF: 123.456.789-01
        "{L3}-{d4}"              → Placa: ABC-1234
        "ID-{d6}"                → Código: ID-123456
        "({d2}) {d?}{d4}-{d4}"   → Telefone com 9 opcional
    """
    
    @staticmethod
    def _parse_placeholder(placeholder: str) -> Tuple[str, Optional[int], bool, bool]:
        """
        Parse de um placeholder.
        
        Returns:
            (tipo, quantidade, is_opcional, is_variavel)
            
        Exemplos:
            "d"   → ("d", None, False, False)  # 1 dígito
            "d3"  → ("d", 3, False, False)     # 3 dígitos
            "d?"  → ("d", None, True, False)   # 0 ou 1 dígito
            "d3?" → ("d", 3, True, False)      # 0 ou 3 dígitos
            "D"   → ("d", None, False, True)   # 1+ dígitos
            "D?"  → ("d", None, True, True)    # 0+ dígitos
        """
        is_opcional = placeholder.endswith('?')
        if is_opcional:
            placeholder = placeholder[:-1]
        
        tipo = placeholder[0].lower()
        is_variavel = placeholder[0].isupper() and placeholder[0] in 'DLW'
        
        quantidade = None
        if len(placeholder) > 1:
            num_str = placeholder[1:]
            if num_str.isdigit():
                quantidade = int(num_str)
        
        return tipo, quantidade, is_opcional, is_variavel
    
    @staticmethod
    def from_template(
        nome: str,
        template: str,
        descricao: str = "",
        pre_process: Optional[str] = None
    ) -> NormalizationRule:
        """
        Cria regra a partir de template.
        
        Args:
            nome: Identificador único da regra
            template: Template com placeholders ({d3}, {L}, {d?}, etc.)
            descricao: Descrição legível
            pre_process: Pré-processamento (auto-detectado se None)
            
        Returns:
            NormalizationRule configurada
        """
        pattern = r'\{([^}]+)\}'
        
        regex_parts = []
        template_parts = []
        case_transforms = {}
        group_num = 1
        last_end = 0
        
        has_digits = False
        has_letters = False
        
        for match in re.finditer(pattern, template):
            # Texto literal antes do placeholder
            literal_before = template[last_end:match.start()]
            if literal_before:
                template_parts.append(literal_before)
            
            placeholder = match.group(1)
            last_end = match.end()
            
            # Parse do placeholder
            if placeholder.startswith('"') and placeholder.endswith('"'):
                # Literal: {"ABC"}
                literal_text = placeholder[1:-1]
                template_parts.append(literal_text)
                continue
            
            # Parse estruturado do placeholder
            is_opcional = placeholder.endswith('?')
            clean_placeholder = placeholder[:-1] if is_opcional else placeholder
            
            tipo = clean_placeholder[0]
            is_variavel = tipo.isupper() and tipo in 'DLW'
            tipo_lower = tipo.lower()
            
            quantidade = None
            if len(clean_placeholder) > 1:
                num_str = clean_placeholder[1:]
                if num_str.isdigit():
                    quantidade = int(num_str)
            
            # Determina o padrão de caractere
            if tipo_lower == 'd':
                has_digits = True
                char_pattern = "\\d"
            elif tipo_lower == 'l':
                has_letters = True
                char_pattern = "[A-Za-z]"
                case_transforms[f"g{group_num}"] = "lower" if tipo == 'l' else "upper"
            elif tipo_lower == 'w':
                has_digits = True
                has_letters = True
                char_pattern = "\\w"
            else:
                continue  # Placeholder desconhecido
            
            # Monta o quantificador
            if is_variavel:
                # D, L, W = quantidade variável
                if is_opcional:
                    quant = "*"  # 0 ou mais
                else:
                    quant = "+"  # 1 ou mais
            elif quantidade:
                # d3, l2, w4 = quantidade fixa
                if is_opcional:
                    quant = f"{{0,{quantidade}}}"  # 0 até N
                else:
                    quant = f"{{{quantidade}}}"  # exatamente N
            else:
                # d, l, w = exatamente 1
                if is_opcional:
                    quant = "?"  # 0 ou 1
                else:
                    quant = ""  # exatamente 1
            
            # Monta regex do grupo
            regex_parts.append(f"(?P<g{group_num}>{char_pattern}{quant})")
            template_parts.append(f"{{g{group_num}}}")
            group_num += 1
        
        # Texto literal após o último placeholder
        if last_end < len(template):
            template_parts.append(template[last_end:])
        
        regex_entrada = ''.join(regex_parts)
        template_saida = ''.join(template_parts)
        
        # Auto-detecta pré-processamento
        if pre_process is None:
            if has_digits and not has_letters:
                pre_process = "digits_only"
            elif has_letters and not has_digits:
                pre_process = "letters_only"
            else:
                pre_process = "alphanumeric"
        
        return NormalizationRule(
            nome=nome,
            descricao=descricao or f"Normaliza para: {template}",
            regex_entrada=regex_entrada,
            template_saida=template_saida,
            pre_process=pre_process,
            case_transforms=case_transforms if case_transforms else None
        )
    
    @staticmethod
    def validate_template(template: str) -> Tuple[bool, Optional[str]]:
        """
        Valida se um template é válido.
        
        Args:
            template: Template a validar
            
        Returns:
            (is_valid, error_message)
        """
        valid_placeholders = {'d', 'D', 'l', 'L', 'w', 'W'}
        pattern = r'\{([^}]+)\}'
        
        for match in re.finditer(pattern, template):
            placeholder = match.group(1)
            
            # Literal entre aspas
            if placeholder.startswith('"') and placeholder.endswith('"'):
                continue
            
            # Placeholder padrão
            if not placeholder:
                return False, f"Placeholder vazio na posição {match.start()}"
            
            # Remove ? do final se existir (indica opcional)
            clean_placeholder = placeholder.rstrip('?')
            
            if not clean_placeholder:
                return False, f"Placeholder inválido: {{{placeholder}}}"
            
            base = clean_placeholder[0]
            if base not in valid_placeholders:
                return False, f"Placeholder inválido: {{{placeholder}}}. Use: d, D, l, L, w, W"
            
            # Verifica número opcional
            if len(clean_placeholder) > 1:
                num_part = clean_placeholder[1:]
                if not num_part.isdigit():
                    return False, f"Quantidade inválida em {{{placeholder}}}. Use número após a letra, opcionalmente seguido de ?"
        
        # Verifica chaves balanceadas
        open_count = template.count('{')
        close_count = template.count('}')
        if open_count != close_count:
            return False, "Chaves desbalanceadas no template"
        
        return True, None


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def _remove_accents(text: str) -> str:
    """Remove acentos de uma string."""
    import unicodedata
    # Decompõe caracteres acentuados e remove os diacríticos
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))


# ============================================================
# NORMALIZER
# ============================================================

class Normalizer:
    """
    Motor de normalização de dados.
    
    Exemplo:
        normalizer = Normalizer()
        normalizer.load_default_rules()
        
        result = normalizer.normalize("12345678901", "cpf")
        print(result)  # "123.456.789-01"
    """
    
    # Pré-processadores disponíveis
    PRE_PROCESSORS = {
        # Filtros (removem caracteres)
        "digits_only": lambda v: re.sub(r"\D", "", v),
        "letters_only": lambda v: re.sub(r"[^a-zA-Z]", "", v),
        "alphanumeric": lambda v: re.sub(r"[^a-zA-Z0-9]", "", v),
        "no_spaces": lambda v: v.replace(" ", ""),
        "strip": lambda v: v.strip(),
        
        # Transformações de case
        "uppercase": lambda v: v.upper(),
        "lowercase": lambda v: v.lower(),
        "capitalize": lambda v: v.capitalize(),           # Primeiro char maiúsculo, resto minúsculo
        "title": lambda v: v.title(),                     # Primeira Letra De Cada Palavra Maiúscula
        "swapcase": lambda v: v.swapcase(),               # Inverte maiúsculas/minúsculas
        
        # Normalização de texto
        "normalize_spaces": lambda v: " ".join(v.split()),  # Múltiplos espaços → 1 espaço
        "remove_accents": lambda v: _remove_accents(v),     # Remove acentos (ção → cao)
    }
    
    def __init__(self):
        self.rules: Dict[str, NormalizationRule] = {}
    
    def add_rule(self, rule: NormalizationRule) -> None:
        """Adiciona uma regra."""
        self.rules[rule.nome] = rule
    
    def remove_rule(self, nome: str) -> bool:
        """Remove uma regra. Retorna True se existia."""
        if nome in self.rules:
            del self.rules[nome]
            return True
        return False
    
    def get_rule(self, nome: str) -> Optional[NormalizationRule]:
        """Retorna uma regra pelo nome."""
        return self.rules.get(nome)
    
    def list_rules(self) -> List[Dict[str, Any]]:
        """Lista todas as regras como dicionários."""
        return [r.to_dict() for r in self.rules.values()]
    
    def normalize(self, value: str, rule_name: str) -> Optional[str]:
        """
        Normaliza um valor usando a regra especificada.
        
        Args:
            value: Valor a normalizar
            rule_name: Nome da regra a aplicar
            
        Returns:
            Valor normalizado ou None se falhar
        """
        if not value or rule_name not in self.rules:
            return None
        
        rule = self.rules[rule_name]
        processed = str(value)
        
        # Pré-processamento
        if rule.pre_process:
            for proc in rule.pre_process.split("|"):
                proc = proc.strip()
                if proc in self.PRE_PROCESSORS:
                    processed = self.PRE_PROCESSORS[proc](processed)
        
        # Aplica regex
        pattern = re.compile(rule.regex_entrada)
        match = pattern.match(processed)
        
        if not match:
            return None
        
        # Extrai grupos
        groups = match.groupdict()
        
        # Aplica transformações de case
        if rule.case_transforms:
            for group_name, transform in rule.case_transforms.items():
                if group_name in groups and groups[group_name]:
                    if transform == "upper":
                        groups[group_name] = groups[group_name].upper()
                    elif transform == "lower":
                        groups[group_name] = groups[group_name].lower()
        
        # Aplica template
        try:
            return rule.template_saida.format(**groups)
        except KeyError:
            return None
    
    def normalize_safe(self, value: str, rule_name: str) -> NormalizationResult:
        """
        Normaliza com retorno detalhado (para APIs).
        
        Args:
            value: Valor a normalizar
            rule_name: Nome da regra
            
        Returns:
            NormalizationResult com detalhes da operação
        """
        if not value:
            return NormalizationResult(
                sucesso=False,
                valor_original=value or "",
                valor_normalizado=None,
                erro="Valor vazio"
            )
        
        if rule_name not in self.rules:
            return NormalizationResult(
                sucesso=False,
                valor_original=value,
                valor_normalizado=None,
                erro=f"Regra '{rule_name}' não encontrada"
            )
        
        try:
            result = self.normalize(value, rule_name)
            if result:
                return NormalizationResult(
                    sucesso=True,
                    valor_original=value,
                    valor_normalizado=result,
                    regra_aplicada=rule_name
                )
            else:
                return NormalizationResult(
                    sucesso=False,
                    valor_original=value,
                    valor_normalizado=None,
                    regra_aplicada=rule_name,
                    erro="Valor não corresponde ao padrão esperado"
                )
        except Exception as e:
            return NormalizationResult(
                sucesso=False,
                valor_original=value,
                valor_normalizado=None,
                erro=str(e)
            )
    
    def normalize_batch(
        self, 
        values: List[str], 
        rule_name: str
    ) -> List[NormalizationResult]:
        """
        Normaliza uma lista de valores.
        
        Args:
            values: Lista de valores
            rule_name: Nome da regra
            
        Returns:
            Lista de NormalizationResult
        """
        return [self.normalize_safe(v, rule_name) for v in values]
    
    def load_default_rules(self) -> None:
        """Carrega regras pré-definidas para tipos comuns."""
        default_rules = [
            # CPF
            RuleBuilder.from_template(
                "cpf_formatado",
                "{d3}.{d3}.{d3}-{d2}",
                "CPF com pontuação (000.000.000-00)"
            ),
            RuleBuilder.from_template(
                "cpf_limpo",
                "{d11}",
                "CPF apenas dígitos (00000000000)"
            ),
            
            # CNPJ
            RuleBuilder.from_template(
                "cnpj_formatado",
                "{d2}.{d3}.{d3}/{d4}-{d2}",
                "CNPJ com pontuação (00.000.000/0000-00)"
            ),
            RuleBuilder.from_template(
                "cnpj_limpo",
                "{d14}",
                "CNPJ apenas dígitos (00000000000000)"
            ),
            
            # Telefone
            RuleBuilder.from_template(
                "telefone_formatado",
                "({d2}) {d5}-{d4}",
                "Telefone celular ((00) 00000-0000)"
            ),
            RuleBuilder.from_template(
                "telefone_fixo_formatado",
                "({d2}) {d4}-{d4}",
                "Telefone fixo ((00) 0000-0000)"
            ),
            RuleBuilder.from_template(
                "telefone_e164",
                "+55{d11}",
                "Telefone E.164 (+5500000000000)"
            ),
            
            # CEP
            RuleBuilder.from_template(
                "cep_formatado",
                "{d5}-{d3}",
                "CEP com hífen (00000-000)"
            ),
            RuleBuilder.from_template(
                "cep_limpo",
                "{d8}",
                "CEP apenas dígitos (00000000)"
            ),
            
            # Data
            RuleBuilder.from_template(
                "data_iso",
                "{d4}-{d2}-{d2}",
                "Data ISO (YYYY-MM-DD)"
            ),
            RuleBuilder.from_template(
                "data_br",
                "{d2}/{d2}/{d4}",
                "Data brasileira (DD/MM/YYYY)"
            ),
            
            # Placa
            RuleBuilder.from_template(
                "placa_antiga",
                "{L3}-{d4}",
                "Placa antiga (ABC-1234)"
            ),
            RuleBuilder.from_template(
                "placa_mercosul",
                "{L3}{d}{L}{d2}",
                "Placa Mercosul (ABC1D23)"
            ),
        ]
        
        for rule in default_rules:
            self.add_rule(rule)
    
    def export_rules_json(self) -> str:
        """Exporta regras como JSON."""
        import json
        return json.dumps(self.list_rules(), indent=2, ensure_ascii=False)
    
    def import_rules_json(self, json_str: str) -> int:
        """
        Importa regras de JSON.
        
        Returns:
            Número de regras importadas
        """
        import json
        rules_data = json.loads(json_str)
        count = 0
        for data in rules_data:
            self.add_rule(NormalizationRule.from_dict(data))
            count += 1
        return count


# ============================================================
# FUNÇÕES UTILITÁRIAS PARA API
# ============================================================

def create_rule_from_request(data: Dict[str, Any]) -> Tuple[Optional[NormalizationRule], Optional[str]]:
    """
    Cria uma regra a partir de dados de request (API).
    
    Args:
        data: Dict com 'nome', 'template', 'descricao' (opcional)
        
    Returns:
        (rule, error_message) - rule é None se houver erro
    """
    nome = data.get("nome")
    template = data.get("template")
    descricao = data.get("descricao", "")
    
    if not nome:
        return None, "Campo 'nome' é obrigatório"
    
    if not template:
        return None, "Campo 'template' é obrigatório"
    
    # Valida template
    is_valid, error = RuleBuilder.validate_template(template)
    if not is_valid:
        return None, error
    
    try:
        rule = RuleBuilder.from_template(nome, template, descricao)
        return rule, None
    except Exception as e:
        return None, str(e)


def get_template_help() -> Dict[str, Any]:
    """
    Retorna documentação dos placeholders (para exibir no frontend).
    """
    return {
        "placeholders": [
            # Obrigatórios
            {"codigo": "{d}", "descricao": "1 dígito", "exemplo": "5", "obrigatorio": True},
            {"codigo": "{d3}", "descricao": "3 dígitos", "exemplo": "123", "obrigatorio": True},
            {"codigo": "{D}", "descricao": "1 ou mais dígitos", "exemplo": "1, 12, 12345...", "obrigatorio": True},
            {"codigo": "{l}", "descricao": "1 letra minúscula", "exemplo": "a", "obrigatorio": True},
            {"codigo": "{l3}", "descricao": "3 letras minúsculas", "exemplo": "abc", "obrigatorio": True},
            {"codigo": "{L}", "descricao": "1 letra MAIÚSCULA", "exemplo": "A", "obrigatorio": True},
            {"codigo": "{L3}", "descricao": "3 letras MAIÚSCULAS", "exemplo": "ABC", "obrigatorio": True},
            {"codigo": "{w}", "descricao": "1 alfanumérico", "exemplo": "a, 1, X", "obrigatorio": True},
            {"codigo": "{w3}", "descricao": "3 alfanuméricos", "exemplo": "a1B", "obrigatorio": True},
            {"codigo": "{W}", "descricao": "1+ alfanuméricos", "exemplo": "abc123...", "obrigatorio": True},
            # Opcionais (com ?)
            {"codigo": "{d?}", "descricao": "0 ou 1 dígito (opcional)", "exemplo": "'' ou 5", "obrigatorio": False},
            {"codigo": "{d3?}", "descricao": "0 a 3 dígitos (opcional)", "exemplo": "'' ou 123", "obrigatorio": False},
            {"codigo": "{D?}", "descricao": "0 ou mais dígitos (opcional)", "exemplo": "'' ou 12345", "obrigatorio": False},
            {"codigo": "{L?}", "descricao": "0 ou 1 letra maiúscula (opcional)", "exemplo": "'' ou A", "obrigatorio": False},
            {"codigo": "{L3?}", "descricao": "0 a 3 letras maiúsculas (opcional)", "exemplo": "'' ou ABC", "obrigatorio": False},
        ],
        "exemplos": [
            {"nome": "CPF", "template": "{d3}.{d3}.{d3}-{d2}", "descricao": "CPF formatado"},
            {"nome": "CNPJ", "template": "{d2}.{d3}.{d3}/{d4}-{d2}", "descricao": "CNPJ formatado"},
            {"nome": "Telefone Celular", "template": "({d2}) {d?}{d4}-{d4}", "descricao": "Com 9 opcional"},
            {"nome": "CEP", "template": "{d5}-{d3}", "descricao": "CEP formatado"},
            {"nome": "Placa", "template": "{L3}-{d4}", "descricao": "Placa antiga"},
            {"nome": "Código", "template": "ID-{d6}", "descricao": "Com prefixo fixo"},
            {"nome": "Protocolo", "template": "{L2?}-{d6}", "descricao": "Prefixo opcional"},
        ],
        "dicas": [
            "Tudo fora das chaves {} aparece literal na saída",
            "O número após a letra indica quantidade exata (ex: {d3} = 3 dígitos)",
            "Letra maiúscula (D, L, W) significa quantidade variável (1 ou mais)",
            "{L} converte para MAIÚSCULA, {l} para minúscula",
            "Adicione ? no final para tornar opcional (ex: {d?} = 0 ou 1 dígito)",
            "{d3?} significa '0 a 3 dígitos' - útil para partes opcionais",
        ]
    }


# ============================================================
# EXEMPLO DE USO
# ============================================================

if __name__ == "__main__":
    # Demonstração
    print("=" * 60)
    print("  DATA NORMALIZER - Demonstração Completa")
    print("=" * 60)
    
    # Criar normalizer
    normalizer = Normalizer()
    normalizer.load_default_rules()
    
    # =========================================================
    # 1. NORMALIZAÇÕES BÁSICAS (regras pré-definidas)
    # =========================================================
    print("\n" + "=" * 60)
    print("  1. NORMALIZAÇÕES BÁSICAS")
    print("=" * 60)
    
    testes_basicos = [
        # CPF
        ("12345678901", "cpf_formatado", "CPF só dígitos"),
        ("123.456.789-01", "cpf_formatado", "CPF já formatado"),
        ("  123 456 789 01  ", "cpf_formatado", "CPF com espaços"),
        
        # CNPJ
        ("12345678000195", "cnpj_formatado", "CNPJ só dígitos"),
        ("12.345.678/0001-95", "cnpj_formatado", "CNPJ já formatado"),
        
        # Telefone
        ("61999998888", "telefone_formatado", "Celular só dígitos"),
        ("(61) 99999-8888", "telefone_formatado", "Celular formatado"),
        
        # CEP
        ("70000000", "cep_formatado", "CEP só dígitos"),
        ("70000-000", "cep_formatado", "CEP já formatado"),
        
        # Placa
        ("abc1234", "placa_antiga", "Placa minúscula"),
        ("ABC1234", "placa_antiga", "Placa maiúscula"),
        ("ABC-1234", "placa_antiga", "Placa com hífen"),
        ("abc1d23", "placa_mercosul", "Mercosul minúscula"),
        ("ABC1D23", "placa_mercosul", "Mercosul maiúscula"),
    ]
    
    for valor, regra, desc in testes_basicos:
        resultado = normalizer.normalize(valor, regra)
        print(f"  {desc}:")
        print(f"    '{valor}' → '{resultado}'")
    
    # =========================================================
    # 2. PARTES OPCIONAIS (?)
    # =========================================================
    print("\n" + "=" * 60)
    print("  2. PARTES OPCIONAIS (?)")
    print("=" * 60)
    
    # Telefone com 9 opcional
    regra_tel = RuleBuilder.from_template(
        "telefone_9_opcional",
        "({d2}) {d?}{d4}-{d4}",
        "Telefone com 9 opcional"
    )
    normalizer.add_rule(regra_tel)
    
    print(f"\n  Template: ({{d2}}) {{d?}}{{d4}}-{{d4}}")
    print(f"  Regex: {regra_tel.regex_entrada}")
    print("  Exemplos:")
    for tel in ["6199998888", "61999998888", "(61) 9999-8888", "(61) 99999-8888"]:
        result = normalizer.normalize(tel, "telefone_9_opcional")
        print(f"    '{tel}' → '{result}'")
    
    # Código com prefixo opcional
    regra_codigo = RuleBuilder.from_template(
        "codigo_prefixo_opcional",
        "{L2?}-{d6}",
        "Código com prefixo opcional"
    )
    normalizer.add_rule(regra_codigo)
    
    print(f"\n  Template: {{L2?}}-{{d6}}")
    print(f"  Regex: {regra_codigo.regex_entrada}")
    print("  Exemplos:")
    for codigo in ["123456", "AB123456", "ab-123456"]:
        result = normalizer.normalize(codigo, "codigo_prefixo_opcional")
        print(f"    '{codigo}' → '{result}'")
    
    # =========================================================
    # 3. PRÉ-PROCESSAMENTOS
    # =========================================================
    print("\n" + "=" * 60)
    print("  3. PRÉ-PROCESSAMENTOS")
    print("=" * 60)
    
    # Demonstra cada pré-processador
    pre_process_exemplos = [
        ("digits_only", "ABC-123.456", "123456"),
        ("letters_only", "ABC-123.def", "ABCdef"),
        ("alphanumeric", "ABC-123.def!", "ABC123def"),
        ("uppercase", "João Silva", "JOÃO SILVA"),
        ("lowercase", "JOÃO SILVA", "joão silva"),
        ("capitalize", "jOÃO sILVA", "João silva"),
        ("title", "joão da silva", "João Da Silva"),
        ("strip", "  texto  ", "texto"),
        ("no_spaces", "a b c d", "abcd"),
        ("normalize_spaces", "joão   da    silva", "joão da silva"),
        ("remove_accents", "São Paulo - Ação", "Sao Paulo - Acao"),
    ]
    
    print("\n  Pré-processadores disponíveis:")
    for proc, entrada, esperado in pre_process_exemplos:
        func = Normalizer.PRE_PROCESSORS[proc]
        resultado = func(entrada)
        status = "✓" if resultado == esperado else "✗"
        print(f"    {status} {proc}:")
        print(f"        '{entrada}' → '{resultado}'")
    
    # =========================================================
    # 4. COMBINANDO PRÉ-PROCESSAMENTOS
    # =========================================================
    print("\n" + "=" * 60)
    print("  4. COMBINANDO PRÉ-PROCESSAMENTOS")
    print("=" * 60)
    
    # Nome próprio
    regra_nome = RuleBuilder.from_template(
        "nome_proprio",
        "{W}",
        "Nome próprio capitalizado",
        pre_process="strip|normalize_spaces|title"
    )
    normalizer.add_rule(regra_nome)
    
    print(f"\n  Template: {{W}}")
    print(f"  Pre-process: strip|normalize_spaces|title")
    print("  Exemplos:")
    for nome in ["  jOÃO   dA   sILVA  ", "MARIA SANTOS", "pedro oliveira"]:
        result = normalizer.normalize(nome, "nome_proprio")
        print(f"    '{nome}' → '{result}'")
    
    # Código sem acentos
    regra_codigo_limpo = RuleBuilder.from_template(
        "codigo_limpo",
        "{W}",
        "Código sem acentos e maiúsculo",
        pre_process="remove_accents|uppercase|alphanumeric"
    )
    normalizer.add_rule(regra_codigo_limpo)
    
    print(f"\n  Template: {{W}}")
    print(f"  Pre-process: remove_accents|uppercase|alphanumeric")
    print("  Exemplos:")
    for codigo in ["São-Paulo/01", "Ação-Teste", "código_123"]:
        result = normalizer.normalize(codigo, "codigo_limpo")
        print(f"    '{codigo}' → '{result}'")
    
    # =========================================================
    # 5. TRANSFORMAÇÕES DE CASE ({L} e {l})
    # =========================================================
    print("\n" + "=" * 60)
    print("  5. TRANSFORMAÇÕES DE CASE ({L} e {l})")
    print("=" * 60)
    
    # Placa sempre maiúscula
    regra_placa = RuleBuilder.from_template(
        "placa_upper",
        "{L3}-{d4}",
        "Placa sempre maiúscula"
    )
    normalizer.add_rule(regra_placa)
    
    print(f"\n  Template: {{L3}}-{{d4}} (L = maiúscula)")
    print("  Exemplos:")
    for placa in ["abc1234", "ABC1234", "AbC-1234"]:
        result = normalizer.normalize(placa, "placa_upper")
        print(f"    '{placa}' → '{result}'")
    
    # Código com prefixo minúsculo
    regra_lower = RuleBuilder.from_template(
        "codigo_lower",
        "{l3}-{d4}",
        "Código com prefixo minúsculo"
    )
    normalizer.add_rule(regra_lower)
    
    print(f"\n  Template: {{l3}}-{{d4}} (l = minúscula)")
    print("  Exemplos:")
    for codigo in ["ABC1234", "abc1234", "AbC-1234"]:
        result = normalizer.normalize(codigo, "codigo_lower")
        print(f"    '{codigo}' → '{result}'")
    
    # =========================================================
    # 6. QUANTIDADE VARIÁVEL ({D}, {L}, {W})
    # =========================================================
    print("\n" + "=" * 60)
    print("  6. QUANTIDADE VARIÁVEL ({D}, {L}, {W})")
    print("=" * 60)
    
    # Número de nota fiscal (variável)
    regra_nf = RuleBuilder.from_template(
        "nota_fiscal",
        "NF-{D}",
        "Nota fiscal com número variável"
    )
    normalizer.add_rule(regra_nf)
    
    print(f"\n  Template: NF-{{D}} (D = 1+ dígitos)")
    print("  Exemplos:")
    for nf in ["123", "123456", "123456789"]:
        result = normalizer.normalize(nf, "nota_fiscal")
        print(f"    '{nf}' → '{result}'")
    
    # Sigla + número
    regra_sigla = RuleBuilder.from_template(
        "sigla_numero",
        "{L}-{D}",
        "Sigla maiúscula + número"
    )
    normalizer.add_rule(regra_sigla)
    
    print(f"\n  Template: {{L}}-{{D}} (L = 1+ letras maiúsculas)")
    print("  Exemplos:")
    for sigla in ["a1", "ab12", "abc123", "ABCD-1234"]:
        result = normalizer.normalize(sigla, "sigla_numero")
        print(f"    '{sigla}' → '{result}'")
    
    # =========================================================
    # 7. VALIDAÇÃO DE TEMPLATES
    # =========================================================
    print("\n" + "=" * 60)
    print("  7. VALIDAÇÃO DE TEMPLATES")
    print("=" * 60)
    
    templates_teste = [
        ("{d3}.{d3}.{d3}-{d2}", True, "CPF válido"),
        ("{d3?}.{d3}", True, "Com opcional"),
        ("{D?}", True, "Variável opcional"),
        ("{L3}-{d4}", True, "Placa válida"),
        ("{x3}", False, "Placeholder inválido"),
        ("{d3", False, "Chave não fechada"),
        ("{}", False, "Placeholder vazio"),
        ("{?}", False, "Só interrogação"),
    ]
    
    print("\n  Validando templates:")
    for template, esperado, desc in templates_teste:
        valid, erro = RuleBuilder.validate_template(template)
        status = "✓" if valid else "✗"
        check = "OK" if valid == esperado else "ERRO"
        if valid:
            print(f"    {status} [{check}] '{template}' - {desc}")
        else:
            print(f"    {status} [{check}] '{template}' - {desc} ({erro})")
    
    # =========================================================
    # 8. EXPORTAR/IMPORTAR REGRAS (JSON)
    # =========================================================
    print("\n" + "=" * 60)
    print("  8. EXPORTAR REGRAS (JSON)")
    print("=" * 60)
    
    # Exporta algumas regras
    json_export = normalizer.export_rules_json()
    print(f"\n  Total de regras: {len(normalizer.rules)}")
    print(f"  Primeiras regras no JSON:")
    import json
    regras_list = json.loads(json_export)
    for r in regras_list[:3]:
        print(f"    - {r['nome']}: {r['descricao']}")
    
    # =========================================================
    # 9. RESUMO
    # =========================================================
    print("\n" + "=" * 60)
    print("  RESUMO")
    print("=" * 60)
    
    help_data = get_template_help()
    print(f"\n  Placeholders disponíveis: {len(help_data['placeholders'])}")
    print(f"  Pré-processadores: {len(Normalizer.PRE_PROCESSORS)}")
    print(f"  Regras carregadas: {len(normalizer.rules)}")
    print(f"  Exemplos prontos: {len(help_data['exemplos'])}")
    print(f"  Dicas: {len(help_data['dicas'])}")
    
    print("\n" + "=" * 60)
    print("  FIM DA DEMONSTRAÇÃO")
    print("=" * 60)

