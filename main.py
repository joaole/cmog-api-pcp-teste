from model.pcp import PCPClient


if __name__ == "__main__":
    cliente = PCPClient()

    resultado = cliente.requisitar(
        "processosAbertos",
        {"cdSituacao": "1", "uf": "SP"}
    )
    if resultado:
        print(resultado)
