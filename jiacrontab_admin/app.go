// iris web api
package admin

import (
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/iwannay/log"
	"github.com/kataras/iris"
	"github.com/kataras/iris/middleware/logger"

	"jiacrontab/models"

	"fmt"

	"github.com/dgrijalva/jwt-go"
	"github.com/iris-contrib/middleware/cors"
	jwtmiddleware "github.com/iris-contrib/middleware/jwt"
	"github.com/kataras/iris/context"
)

// newApp iris工厂函数
func newApp(adm *Admin) *iris.Application {

	app := iris.New()
	//而使用 ‘UseGlobal’ 方法注册的中间件，会在包括所有子域名在内的所有路由中执行
	app.UseGlobal(newRecover(adm)) // 重定义ctx，加入自定义的内容，主要做web stat统计
	app.Logger().SetLevel(adm.getOpts().App.LogLevel)
	//使用 ‘Use’ 方法作为当前域名下所有路由的第一个处理函数
	//注意 Use 和 Done 方法需要写在绑定访问路径的方法之前
	app.Use(logger.New())
	// 这里使用gzip植入式方式来提供static请求
	app.StaticEmbeddedGzip("/", "./assets/", GzipAsset, GzipAssetNames)
	cfg := adm.getOpts()

	// 包装路由器，将adm实例封装到ctx上下文
	wrapHandler := func(h func(ctx *myctx)) context.Handler {
		return func(c iris.Context) {
			h(wrapCtx(c, adm))
		}
	}
	// jwt json web token 初始化jwt中间件handle
	jwtHandler := jwtmiddleware.New(jwtmiddleware.Config{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return []byte(cfg.Jwt.SigningKey), nil
		},

		Extractor: func(ctx iris.Context) (string, error) {
			token, err := url.QueryUnescape(ctx.GetHeader(cfg.Jwt.Name))
			return token, err
		},
		Expiration: true,

		ErrorHandler: func(c iris.Context, err error) {
			ctx := wrapCtx(c, adm)
			if ctx.RequestPath(true) != "/user/login" {
				ctx.respAuthFailed(fmt.Errorf("Token verification failed(%s)", err))
				return
			}
			ctx.Next()
		},

		SigningMethod: jwt.SigningMethodHS256,
	})
	// 跨域访问，解决前端调用后端问题
	crs := cors.New(cors.Options{
		Debug:            true,
		AllowedHeaders:   []string{"Content-Type", "Token"},
		AllowedOrigins:   []string{"*"}, // allows everything, use that to change the hosts.
		AllowCredentials: true,
	})

	app.Use(crs) // app引用跨域配置
	app.AllowMethods(iris.MethodOptions)
	app.Get("/", func(ctx iris.Context) {
		if atomic.LoadInt32(&adm.initAdminUser) == 1 {
			ctx.SetCookieKV("ready", "true", func(c *http.Cookie) {
				c.HttpOnly = false
			})
		} else {
			ctx.SetCookieKV("ready", "false", func(c *http.Cookie) {
				c.HttpOnly = false
			})
		}
		ctx.Header("Cache-Control", "no-cache")
		ctx.Header("Content-Type", "text/html; charset=utf-8")
		ctx.Header("Content-Encoding", "gzip")
		ctx.Header("Vary", "Accept-Encoding")
		asset, err := GzipAsset("assets/index.html")
		if err != nil {
			log.Error(err)
		}
		ctx.Write(asset)
	})
	// v1 没有jwt鉴权
	v1 := app.Party("/v1")
	{
		v1.Post("/user/login", wrapHandler(Login))
		v1.Post("/app/init", wrapHandler(InitApp))
	}

	v2 := app.Party("/v2")
	{
		v2.Use(jwtHandler.Serve) // v2的party引用jwt鉴权中间件
		v2.Use(wrapHandler(func(ctx *myctx) {
			if err := ctx.parseClaimsFromToken(); err != nil {
				ctx.respJWTError(err)
				return
			}
			ctx.Next()
		}))
		v2.Post("/crontab/job/list", wrapHandler(GetJobList))
		v2.Post("/crontab/job/get", wrapHandler(GetJob))
		v2.Post("/crontab/job/log", wrapHandler(GetRecentLog))
		v2.Post("/crontab/job/edit", wrapHandler(EditJob))
		v2.Post("/crontab/job/action", wrapHandler(ActionTask))
		v2.Post("/crontab/job/exec", wrapHandler(ExecTask))

		v2.Post("/config/get", wrapHandler(GetConfig))
		v2.Post("/config/mail/send", wrapHandler(SendTestMail))
		v2.Post("/system/info", wrapHandler(SystemInfo))

		v2.Post("/daemon/job/action", wrapHandler(ActionDaemonTask))
		v2.Post("/daemon/job/edit", wrapHandler(EditDaemonJob))
		v2.Post("/daemon/job/get", wrapHandler(GetDaemonJob))
		v2.Post("/daemon/job/log", wrapHandler(GetRecentDaemonLog))

		v2.Post("/group/list", wrapHandler(GetGroupList))
		v2.Post("/group/edit", wrapHandler(EditGroup))

		v2.Post("/node/list", wrapHandler(GetNodeList))
		v2.Post("/node/delete", wrapHandler(DeleteNode))
		v2.Post("/node/group_node", wrapHandler(GroupNode))

		v2.Post("/user/activity_list", wrapHandler(GetActivityList))
		v2.Post("/user/job_history", wrapHandler(GetJobHistory))
		v2.Post("/user/audit_job", wrapHandler(AuditJob))
		v2.Post("/user/stat", wrapHandler(UserStat))
		v2.Post("/user/signup", wrapHandler(Signup))
		v2.Post("/user/edit", wrapHandler(EditUser))
		v2.Post("/user/delete", wrapHandler(DeleteUser))
		v2.Post("/user/group_user", wrapHandler(GroupUser))
		v2.Post("/user/list", wrapHandler(GetUserList))
	}

	debug := app.Party("/debug")
	{
		debug.Get("/stat", wrapHandler(stat))
		debug.Get("/pprof/", wrapHandler(indexDebug))
		debug.Get("/pprof/{key:string}", wrapHandler(pprofHandler))
	}

	return app
}

// InitApp 初始化应用
func InitApp(ctx *myctx) {
	var (
		err     error
		user    models.User
		reqBody InitAppReqParams
	)

	if err = ctx.Valid(&reqBody); err != nil {
		ctx.respParamError(err)
		return
	}

	if ret := models.DB().Take(&user, "group_id=?", 1); ret.Error == nil && ret.RowsAffected > 0 {
		ctx.respNotAllowed()
		return
	}

	user.Username = reqBody.Username
	user.Passwd = reqBody.Passwd
	user.Root = true
	user.GroupID = models.SuperGroup.ID
	user.Mail = reqBody.Mail

	if err = user.Create(); err != nil {
		ctx.respBasicError(err)
		return
	}
	atomic.StoreInt32(&ctx.adm.initAdminUser, 1)
	ctx.respSucc("", true)
}
