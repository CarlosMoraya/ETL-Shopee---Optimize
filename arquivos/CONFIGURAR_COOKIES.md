# 🔐 Configurar Secrets - Cookies Shopee

## 📋 O Que São Cookies?

Cookies são **credenciais de sessão** que o site cria quando você faz login. Eles valem por **7 dias** (até 2026-04-07).

---

## 🎯 Secrets Necessários

Adicione no GitHub: **Settings → Secrets and variables → Actions → New repository secret**

| Secret Name | Valor (Exemplo) | Obrigatório |
|-------------|-----------------|-------------|
| `SHOPEE_FMS_USER_SKEY` | `1oTDSYknsNTkVlhkAmk8dsFFa...` | ✅ SIM |
| `SHOPEE_SPX_UK` | `1oTDSYknsNTkVlhkAmk8dsFFa...` | ✅ SIM |
| `SHOPEE_FMS_USER_ID` | `612983` | ✅ SIM |
| `SHOPEE_FMS_DISPLAY_NAME` | `Carlos%20Eduardo%20Pereira...` | ⚠️ Opcional |
| `SHOPEE_FMS_USER_AGENCY_ID` | `50` | ⚠️ Opcional |
| `SHOPEE_SPX_AGID` | `50` | ⚠️ Opcional |
| `SHOPEE_SPX_CID` | `BR` | ⚠️ Opcional |
| `SHOPEE_SPX_DN` | `Carlos%20Eduardo...` | ⚠️ Opcional |
| `SHOPEE_SPX_ST` | `4` | ⚠️ Opcional |
| `SHOPEE_SPX_UID` | `612983` | ⚠️ Opcional |
| `SHOPEE_ADMIN_DEVICE_ID` | `b8ac60cc3f76757dd74ff0f845e8b5a3` | ⚠️ Opcional |
| `SHOPEE_ADMIN_LANG` | `pt-br` | ⚠️ Opcional |
| `SHOPEE_SSC_USER_ROLE` | `-` | ⚠️ Opcional |

---

## 📸 Como Pegar os Cookies

1. **Abra o Chrome** e acesse: https://logistics.myagencyservice.com.br/
2. **Faça login** com email e senha
3. **Aperte F12** (DevTools)
4. **Vá na aba "Network"**
5. **Clique em qualquer requisição** na lista
6. **Vá na aba "Cookies"** (embaixo)
7. **Copie o valor** de cada cookie
8. **Adicione como Secret** no GitHub

---

## ⚠️ Importante

- **Cookies expiram em 7 dias!**
- **Renove os secrets** toda semana
- **Só precisa dos 3 principais** (`FMS_USER_SKEY`, `SPX_UK`, `FMS_USER_ID`)

---

## 🚀 Depois de Adicionar

1. **Vá em Actions** no GitHub
2. **Rode o workflow** manualmente
3. **Deve funcionar!** ✅

---

## 🔄 Quando Expirar

Quando der erro **401** ou **403** nos logs:

1. **Repita o processo** de pegar cookies
2. **Atualize os secrets** no GitHub
3. **Rode o workflow** novamente
