from typing import List, Dict
import ast
from openai import OpenAI

from app.core.config import settings


async def get_bordering_local_authorities(authority_name: str) -> Dict[str, List[str]]:
    """
    Retrieve a list of UK local authorities that share a direct geographical border with the given authority.

    Args:
        authority_name (str): Name of the local authority

    Returns:
        Dict[str, List[str]]: Dictionary containing a list of bordering local authorities under 'nearest' key

    Raises:
        ValueError: If authority_name is empty or invalid
    """
    if not authority_name or not authority_name.strip():
        return {"nearest": []}

    client = OpenAI(
        base_url=settings.OPEN_ROUTER_URL,
        api_key=settings.OPEN_ROUTER_KEY,
    )

    prompt = (
        f"Given the context of UK local authority boundaries, return a list of local "
        f"authorities that share a direct geographical border with '{authority_name}'. "
        f"Respond only with a Python-style array: ['Name A', 'Name B']"
    )

    try:
        completion = client.chat.completions.create(
            model="qwen/qwen3-14b:free",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )

        if not completion.choices:
            return {"nearest": []}

        content = completion.choices[0].message.content.strip()
        result = ast.literal_eval(content)

        if isinstance(result, list) and all(isinstance(x, str) for x in result):
            return {"nearest": result}

        return {"nearest": []}
    except (ast.ASTError, ValueError, SyntaxError):
        return {"nearest": []}
    except Exception:
        return {"nearest": []}
    finally:
        client.close()