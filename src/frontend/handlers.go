// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/frontend/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/frontend/money"
	"github.com/GoogleCloudPlatform/microservices-demo/src/frontend/validator"
)

// 存储运行环境信息 - 云服务器？本地？
type platformDetails struct {
	css      string
	provider string
}

// 读取一些环境变量并存储为相应的全局变量
// 好吧其实每个都不太知道在做什么以及有什么用
// 答：也不是很重要
var (
	frontendMessage  = strings.TrimSpace(os.Getenv("FRONTEND_MESSAGE"))         // 可以在环境变量里传入标题大字
	isCymbalBrand    = "true" == strings.ToLower(os.Getenv("CYMBAL_BRANDING"))  // 不重要
	assistantEnabled = "true" == strings.ToLower(os.Getenv("ENABLE_ASSISTANT")) // 控制显示AI聊天机器人的入口
	// 答：这个重要，模板初始化
	// 通过FuncMac“挂载”Go的格式化函数到template中，就比如这里注册了renderMoney和renderCurrencyLogo
	templates = template.Must(template.New("").
			Funcs(template.FuncMap{
			"renderMoney":        renderMoney,
			"renderCurrencyLogo": renderCurrencyLogo,
		}).ParseGlob("templates/*.html"))
	plat platformDetails
)

// 有效的运行环境
var validEnvs = []string{"local", "gcp", "azure", "aws", "onprem", "alibaba"}

// 首页
// 大概是生成日志器，创建上下文，远程调用一些函数，最终导入到template数据中并加载
func (fe *frontendServer) homeHandler(w http.ResponseWriter, r *http.Request) {
	// 基于当前http请求的上下文创建一个日志器
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	// 引入计数器，记录请求数量
	if requestCounter != nil {
		requestCounter.Add(r.Context(), 1)
	}
	log.WithField("currency", currentCurrency(r)).Info("home")
	// 没看懂getCurrencies是干什么的
	// 答：远程调用其他模块的方法，获得当前可用的货币币种
	currencies, err := fe.getCurrencies(r.Context())
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve currencies"), http.StatusInternalServerError)
		return
	}
	// 远程调用其他模块的方法，获得产品列表
	products, err := fe.getProducts(r.Context())
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve products"), http.StatusInternalServerError)
		return
	}
	// 远程调用其他模块的方法，获得购物车信息
	cart, err := fe.getCart(r.Context(), sessionID(r))
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve cart"), http.StatusInternalServerError)
		return
	}

	// 存储产品相关信息
	type productView struct {
		Item  *pb.Product
		Price *pb.Money
	}
	// 创建相应的空切片，以productView结构体为元素
	// 后面的遍历应该是在基于当前上下文的汇率更新产品价格（对的，主要是要根据用户选择的币种更新）
	ps := make([]productView, len(products))
	for i, p := range products {
		price, err := fe.convertCurrency(r.Context(), p.GetPriceUsd(), currentCurrency(r))
		if err != nil {
			renderHTTPError(log, r, w, errors.Wrapf(err, "failed to do currency conversion for product %s", p.GetId()), http.StatusInternalServerError)
			return
		}
		ps[i] = productView{p, price}
	}

	//以下都是运行环境相关的设置，主要是读取环境变量/读取不到就启用默认设置，local或gcp

	// Set ENV_PLATFORM (default to local if not set; use env var if set; otherwise detect GCP, which overrides env)_
	var env = os.Getenv("ENV_PLATFORM")
	// Only override from env variable if set + valid env
	if env == "" || stringinSlice(validEnvs, env) == false {
		fmt.Println("env platform is either empty or invalid")
		env = "local"
	}
	// Autodetect GCP
	addrs, err := net.LookupHost("metadata.google.internal.")
	if err == nil && len(addrs) >= 0 {
		log.Debugf("Detected Google metadata server: %v, setting ENV_PLATFORM to GCP.", addrs)
		env = "gcp"
	}

	// 基于设置好的环境变量中添加并存储运行环境信息
	log.Debugf("ENV_PLATFORM is: %s", env)
	plat = platformDetails{}
	plat.setPlatformDetails(strings.ToLower(env))

	// 在home页面（首页）导入默认模板，并在原基础上添加刚刚获得的新的键值对信息：汇率，产品，购物车等等，通过ResponseWriter写入
	if err := templates.ExecuteTemplate(w, "home", injectCommonTemplateData(r, map[string]interface{}{
		"show_currency": true,
		"currencies":    currencies,
		"products":      ps,
		"cart_size":     cartSize(cart),
		"banner_color":  os.Getenv("BANNER_COLOR"), // illustrates canary deployments
		"ad":            fe.chooseAd(r.Context(), []string{}, log),
	})); err != nil {
		log.Error(err)
	}
}

func (plat *platformDetails) setPlatformDetails(env string) {
	if env == "aws" {
		plat.provider = "AWS"
		plat.css = "aws-platform"
	} else if env == "onprem" {
		plat.provider = "On-Premises"
		plat.css = "onprem-platform"
	} else if env == "azure" {
		plat.provider = "Azure"
		plat.css = "azure-platform"
	} else if env == "gcp" {
		plat.provider = "Google Cloud"
		plat.css = "gcp-platform"
	} else if env == "alibaba" {
		plat.provider = "Alibaba Cloud"
		plat.css = "alibaba-platform"
	} else {
		plat.provider = "local"
		plat.css = "local"
	}
}

// 根据 URL 参数获取特定商品，并处理关联推荐
func (fe *frontendServer) productHandler(w http.ResponseWriter, r *http.Request) {
	// 日志生成器
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	// 从http请求中提取特定的产品ID？
	id := mux.Vars(r)["id"]
	if id == "" {
		renderHTTPError(log, r, w, errors.New("product id not specified"), http.StatusBadRequest)
		return
	}

	// 这个汇率是添加到哪里了？
	// 答：（写进结构化日志）去后台日志系统了，可以理解为OTel的一部分，因为logrus就是个结构化日志库
	log.WithField("id", id).WithField("currency", currentCurrency(r)).
		Debug("serving product page")

	// 以下是老生常谈的远程调用
	// 从http请求中获取并存储产品信息、汇率信息、购物车信息、做了汇率转换后的价格信息、推荐信息
	p, err := fe.getProduct(r.Context(), id)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve product"), http.StatusInternalServerError)
		return
	}
	currencies, err := fe.getCurrencies(r.Context())
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve currencies"), http.StatusInternalServerError)
		return
	}

	cart, err := fe.getCart(r.Context(), sessionID(r))
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve cart"), http.StatusInternalServerError)
		return
	}

	price, err := fe.convertCurrency(r.Context(), p.GetPriceUsd(), currentCurrency(r))
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to convert currency"), http.StatusInternalServerError)
		return
	}

	// ignores the error retrieving recommendations since it is not critical
	recommendations, err := fe.getRecommendations(r.Context(), sessionID(r), []string{id})
	if err != nil {
		log.WithField("error", err).Warn("failed to get product recommendations")
	}

	// 用这个新创建的结构体存储刚刚获取的产品和价格（已经转换汇率）
	product := struct {
		Item  *pb.Product
		Price *pb.Money
	}{p, price}

	// Fetch packaging info (weight/dimensions) of the product
	// The packaging service is an optional microservice you can run as part of a Google Cloud demo.
	// 大概看得懂这个处理逻辑，就是package info是什么？干什么用的
	// 答：商品的物理属性，重量、高度等等，因为算运费（shipping）可能要用
	var packagingInfo *PackagingInfo = nil
	if isPackagingServiceConfigured() {
		packagingInfo, err = httpGetPackagingInfo(id)
		if err != nil {
			fmt.Println("Failed to obtain product's packaging info:", err)
		}
	}

	// 在模板的产品模块添加新的键值对信息
	// 突然想知道这些是最终会直接渲染然后呈现给用户吗
	// 答：对的，直接写进html
	if err := templates.ExecuteTemplate(w, "product", injectCommonTemplateData(r, map[string]interface{}{
		"ad":              fe.chooseAd(r.Context(), p.Categories, log),
		"show_currency":   true,
		"currencies":      currencies,
		"product":         product,
		"recommendations": recommendations,
		"cart_size":       cartSize(cart),
		"packagingInfo":   packagingInfo,
	})); err != nil {
		log.Println(err)
	}
}

// 处理表单提交，验证数据，然后调用联系人写入购物车
func (fe *frontendServer) addToCartHandler(w http.ResponseWriter, r *http.Request) {
	// 日志生成器
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	// 其实没见过这个这几个函数，但大概猜是从http request中寻找相应的字符串，然后从中解析出需要的数值？分别对应数量和产品ID
	// 答：从html表单中根据拿出所需要的数据
	quantity, _ := strconv.ParseUint(r.FormValue("quantity"), 10, 32)
	productID := r.FormValue("product_id")

	// 将这些解析出的信息使用“待加入购物车”结构体存储
	payload := validator.AddToCartPayload{
		Quantity:  quantity,
		ProductID: productID,
	}

	// 那这个payload.Validate()应该就是基于“待加入购物车”结构体的“加入购物车”方法吧
	// 答：并非，是在检查数量有没有问题（0或复数），产品ID是否为空；通过检查了才执行下文的insertCart
	if err := payload.Validate(); err != nil {
		renderHTTPError(log, r, w, validator.ValidationErrorResponse(err), http.StatusUnprocessableEntity)
		return
	}
	log.WithField("product", payload.ProductID).WithField("quantity", payload.Quantity).Debug("adding to cart")

	// 远程调用getProduct方法，获取刚才成功加入购物车的产品
	p, err := fe.getProduct(r.Context(), payload.ProductID)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve product"), http.StatusInternalServerError)
		return
	}

	// 远程调用插入购物车表单的方法，基于sessionID（用于区别用户吧），在购物车表单中添加刚才的产品ID和数量
	if err := fe.insertCart(r.Context(), sessionID(r), p.GetId(), int32(payload.Quantity)); err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to add to cart"), http.StatusInternalServerError)
		return
	}

	// 没太看懂，第一句是在说路由？这路由干啥的，什么时候用的
	// 答：不是路由规则，是在重定向跳转；成功更新购物车表单后就去向"/cart"这个路由发起新的访问请求
	w.Header().Set("location", baseUrl+"/cart")
	w.WriteHeader(http.StatusFound)
}

func (fe *frontendServer) emptyCartHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("emptying cart")

	if err := fe.emptyCart(r.Context(), sessionID(r)); err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to empty cart"), http.StatusInternalServerError)
		return
	}
	w.Header().Set("location", baseUrl+"/")
	w.WriteHeader(http.StatusFound)
}

func (fe *frontendServer) viewCartHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("view user cart")
	currencies, err := fe.getCurrencies(r.Context())
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve currencies"), http.StatusInternalServerError)
		return
	}
	cart, err := fe.getCart(r.Context(), sessionID(r))
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve cart"), http.StatusInternalServerError)
		return
	}

	// ignores the error retrieving recommendations since it is not critical
	recommendations, err := fe.getRecommendations(r.Context(), sessionID(r), cartIDs(cart))
	if err != nil {
		log.WithField("error", err).Warn("failed to get product recommendations")
	}

	shippingCost, err := fe.getShippingQuote(r.Context(), cart, currentCurrency(r))
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to get shipping quote"), http.StatusInternalServerError)
		return
	}

	type cartItemView struct {
		Item     *pb.Product
		Quantity int32
		Price    *pb.Money
	}
	items := make([]cartItemView, len(cart))
	totalPrice := pb.Money{CurrencyCode: currentCurrency(r)}
	for i, item := range cart {
		p, err := fe.getProduct(r.Context(), item.GetProductId())
		if err != nil {
			renderHTTPError(log, r, w, errors.Wrapf(err, "could not retrieve product #%s", item.GetProductId()), http.StatusInternalServerError)
			return
		}
		price, err := fe.convertCurrency(r.Context(), p.GetPriceUsd(), currentCurrency(r))
		if err != nil {
			renderHTTPError(log, r, w, errors.Wrapf(err, "could not convert currency for product #%s", item.GetProductId()), http.StatusInternalServerError)
			return
		}

		multPrice := money.MultiplySlow(*price, uint32(item.GetQuantity()))
		items[i] = cartItemView{
			Item:     p,
			Quantity: item.GetQuantity(),
			Price:    &multPrice}
		totalPrice = money.Must(money.Sum(totalPrice, multPrice))
	}
	totalPrice = money.Must(money.Sum(totalPrice, *shippingCost))
	year := time.Now().Year()

	if err := templates.ExecuteTemplate(w, "cart", injectCommonTemplateData(r, map[string]interface{}{
		"currencies":       currencies,
		"recommendations":  recommendations,
		"cart_size":        cartSize(cart),
		"shipping_cost":    shippingCost,
		"show_currency":    true,
		"total_cost":       totalPrice,
		"items":            items,
		"expiration_years": []int{year, year + 1, year + 2, year + 3, year + 4},
	})); err != nil {
		log.Println(err)
	}
}

// 表单验证、信用卡模拟、调用 Checkout 微服务
func (fe *frontendServer) placeOrderHandler(w http.ResponseWriter, r *http.Request) {
	// 日志生成器
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("placing order")

	// 定义变量，从表单中读取数据并存储
	var (
		email         = r.FormValue("email")
		streetAddress = r.FormValue("street_address")
		zipCode, _    = strconv.ParseInt(r.FormValue("zip_code"), 10, 32)
		city          = r.FormValue("city")
		state         = r.FormValue("state")
		country       = r.FormValue("country")
		ccNumber      = r.FormValue("credit_card_number")
		ccMonth, _    = strconv.ParseInt(r.FormValue("credit_card_expiration_month"), 10, 32)
		ccYear, _     = strconv.ParseInt(r.FormValue("credit_card_expiration_year"), 10, 32)
		ccCVV, _      = strconv.ParseInt(r.FormValue("credit_card_cvv"), 10, 32)
	)

	// 检查验证数据正确性，通过了才执行下文的placeorder
	payload := validator.PlaceOrderPayload{
		Email:         email,
		StreetAddress: streetAddress,
		ZipCode:       zipCode,
		City:          city,
		State:         state,
		Country:       country,
		CcNumber:      ccNumber,
		CcMonth:       ccMonth,
		CcYear:        ccYear,
		CcCVV:         ccCVV,
	}
	if err := payload.Validate(); err != nil {
		renderHTTPError(log, r, w, validator.ValidationErrorResponse(err), http.StatusUnprocessableEntity)
		return
	}

	// 创建checkout服务的客户端，通过客户端向服务端发起request进行结账
	// 在请求中写入变量里读取到并经过检验的数据，如用户ID、信用卡信息、汇率、收货地址等等，主要还是信用卡结账
	order, err := pb.NewCheckoutServiceClient(fe.checkoutSvcConn).
		PlaceOrder(r.Context(), &pb.PlaceOrderRequest{
			Email: payload.Email,
			CreditCard: &pb.CreditCardInfo{
				CreditCardNumber:          payload.CcNumber,
				CreditCardExpirationMonth: int32(payload.CcMonth),
				CreditCardExpirationYear:  int32(payload.CcYear),
				CreditCardCvv:             int32(payload.CcCVV)},
			UserId:       sessionID(r),
			UserCurrency: currentCurrency(r),
			Address: &pb.Address{
				StreetAddress: payload.StreetAddress,
				City:          payload.City,
				State:         payload.State,
				ZipCode:       int32(payload.ZipCode),
				Country:       payload.Country},
		})
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to complete the order"), http.StatusInternalServerError)
		return
	}

	// 结构化日志，手动trace当前会话的order
	log.WithField("order", order.GetOrder().GetOrderId()).Info("order placed")

	// 这里应该是下单后生成相关推荐
	recommendations, _ := fe.getRecommendations(r.Context(), sessionID(r), nil)

	// 计算总价，产品价格+运费
	// 就是这个multPrice和totalPaid是什么说法？为什么要做这个区分
	// 答：分别是小计和总价，另外地，使用MultiplySlow和Sum是因为此处的money是一个结构体，得用专门的乘法和相加函数处理结构体之间的运算
	totalPaid := *order.GetOrder().GetShippingCost()
	for _, v := range order.GetOrder().GetItems() {
		multPrice := money.MultiplySlow(*v.GetCost(), uint32(v.GetItem().GetQuantity()))
		totalPaid = money.Must(money.Sum(totalPaid, multPrice))
	}

	// 转换到当前汇率
	currencies, err := fe.getCurrencies(r.Context())
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve currencies"), http.StatusInternalServerError)
		return
	}

	// 更新模板中order模块的数据
	if err := templates.ExecuteTemplate(w, "order", injectCommonTemplateData(r, map[string]interface{}{
		"show_currency":   false,
		"currencies":      currencies,
		"order":           order.GetOrder(),
		"total_paid":      &totalPaid,
		"recommendations": recommendations,
	})); err != nil {
		log.Println(err)
	}

	// 那到底具体是哪一步执行了结账操作？为什么总价的计算、换汇都还在checkout后面？
	// 答：就是在调用checkout那一步执行了结账，后面计算这些只是要再把信息注入到模板中给用户看
}

func (fe *frontendServer) assistantHandler(w http.ResponseWriter, r *http.Request) {
	currencies, err := fe.getCurrencies(r.Context())
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "could not retrieve currencies"), http.StatusInternalServerError)
		return
	}

	if err := templates.ExecuteTemplate(w, "assistant", injectCommonTemplateData(r, map[string]interface{}{
		"show_currency": false,
		"currencies":    currencies,
	})); err != nil {
		log.Println(err)
	}
}

func (fe *frontendServer) logoutHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("logging out")
	for _, c := range r.Cookies() {
		c.Expires = time.Now().Add(-time.Hour * 24 * 365)
		c.MaxAge = -1
		http.SetCookie(w, c)
	}
	w.Header().Set("Location", baseUrl+"/")
	w.WriteHeader(http.StatusFound)
}

func (fe *frontendServer) getProductByID(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["ids"]
	if id == "" {
		return
	}

	p, err := fe.getProduct(r.Context(), id)
	if err != nil {
		return
	}

	jsonData, err := json.Marshal(p)
	if err != nil {
		fmt.Println(err)
		return
	}

	w.Write(jsonData)
	w.WriteHeader(http.StatusOK)
}

func (fe *frontendServer) chatBotHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	type Response struct {
		Message string `json:"message"`
	}

	type LLMResponse struct {
		Content string         `json:"content"`
		Details map[string]any `json:"details"`
	}

	var response LLMResponse

	url := "http://" + fe.shoppingAssistantSvcAddr
	req, err := http.NewRequest(http.MethodPost, url, r.Body)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to create request"), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to send request"), http.StatusInternalServerError)
		return
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to read response"), http.StatusInternalServerError)
		return
	}

	fmt.Printf("%+v\n", body)
	fmt.Printf("%+v\n", res)

	err = json.Unmarshal(body, &response)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to unmarshal body"), http.StatusInternalServerError)
		return
	}

	// respond with the same message
	json.NewEncoder(w).Encode(Response{Message: response.Content})

	w.WriteHeader(http.StatusOK)
}

func (fe *frontendServer) setCurrencyHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	cur := r.FormValue("currency_code")
	payload := validator.SetCurrencyPayload{Currency: cur}
	if err := payload.Validate(); err != nil {
		renderHTTPError(log, r, w, validator.ValidationErrorResponse(err), http.StatusUnprocessableEntity)
		return
	}
	log.WithField("curr.new", payload.Currency).WithField("curr.old", currentCurrency(r)).
		Debug("setting currency")

	if payload.Currency != "" {
		http.SetCookie(w, &http.Cookie{
			Name:   cookieCurrency,
			Value:  payload.Currency,
			MaxAge: cookieMaxAge,
		})
	}
	referer := r.Header.Get("referer")
	if referer == "" {
		referer = baseUrl + "/"
	}
	w.Header().Set("Location", referer)
	w.WriteHeader(http.StatusFound)
}

// chooseAd queries for advertisements available and randomly chooses one, if
// available. It ignores the error retrieving the ad since it is not critical.
func (fe *frontendServer) chooseAd(ctx context.Context, ctxKeys []string, log logrus.FieldLogger) *pb.Ad {
	ads, err := fe.getAd(ctx, ctxKeys)
	if err != nil {
		log.WithField("error", err).Warn("failed to retrieve ads")
		return nil
	}
	return ads[rand.Intn(len(ads))]
}

func renderHTTPError(log logrus.FieldLogger, r *http.Request, w http.ResponseWriter, err error, code int) {
	log.WithField("error", err).Error("request error")
	errMsg := fmt.Sprintf("%+v", err)

	w.WriteHeader(code)

	if templateErr := templates.ExecuteTemplate(w, "error", injectCommonTemplateData(r, map[string]interface{}{
		"error":       errMsg,
		"status_code": code,
		"status":      http.StatusText(code),
	})); templateErr != nil {
		log.Println(templateErr)
	}
}

func injectCommonTemplateData(r *http.Request, payload map[string]interface{}) map[string]interface{} {
	data := map[string]interface{}{
		"session_id":        sessionID(r),
		"request_id":        r.Context().Value(ctxKeyRequestID{}),
		"user_currency":     currentCurrency(r),
		"platform_css":      plat.css,
		"platform_name":     plat.provider,
		"is_cymbal_brand":   isCymbalBrand,
		"assistant_enabled": assistantEnabled,
		"deploymentDetails": deploymentDetailsMap,
		"frontendMessage":   frontendMessage,
		"currentYear":       time.Now().Year(),
		"baseUrl":           baseUrl,
	}

	for k, v := range payload {
		data[k] = v
	}

	return data
}

func currentCurrency(r *http.Request) string {
	c, _ := r.Cookie(cookieCurrency)
	if c != nil {
		return c.Value
	}
	return defaultCurrency
}

func sessionID(r *http.Request) string {
	v := r.Context().Value(ctxKeySessionID{})
	if v != nil {
		return v.(string)
	}
	return ""
}

func cartIDs(c []*pb.CartItem) []string {
	out := make([]string, len(c))
	for i, v := range c {
		out[i] = v.GetProductId()
	}
	return out
}

// get total # of items in cart
func cartSize(c []*pb.CartItem) int {
	cartSize := 0
	for _, item := range c {
		cartSize += int(item.GetQuantity())
	}
	return cartSize
}

func renderMoney(money pb.Money) string {
	currencyLogo := renderCurrencyLogo(money.GetCurrencyCode())
	return fmt.Sprintf("%s%d.%02d", currencyLogo, money.GetUnits(), money.GetNanos()/10000000)
}

func renderCurrencyLogo(currencyCode string) string {
	logos := map[string]string{
		"USD": "$",
		"CAD": "$",
		"JPY": "¥",
		"EUR": "€",
		"TRY": "₺",
		"GBP": "£",
	}

	logo := "$" //default
	if val, ok := logos[currencyCode]; ok {
		logo = val
	}
	return logo
}

func stringinSlice(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
