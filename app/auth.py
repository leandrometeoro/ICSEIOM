"""Autenticação simples por sessão (cookie assinado)."""
from fastapi import Request, HTTPException, status
from fastapi.responses import RedirectResponse
from .config import ADMIN_USER, ADMIN_PASSWORD


def login_user(request: Request, username: str, password: str) -> bool:
    if username == ADMIN_USER and password == ADMIN_PASSWORD:
        request.session["user"] = username
        return True
    return False


def logout_user(request: Request) -> None:
    request.session.pop("user", None)


def current_user(request: Request) -> str | None:
    return request.session.get("user")


def require_admin(request: Request):
    if not current_user(request):
        raise HTTPException(
            status_code=status.HTTP_303_SEE_OTHER,
            headers={"Location": "/login"},
        )
    return request.session["user"]
