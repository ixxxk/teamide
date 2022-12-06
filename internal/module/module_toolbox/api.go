package module_toolbox

import (
	"teamide/internal/base"
	"teamide/internal/context"
)

type ToolboxApi struct {
	*context.ServerContext
	ToolboxService *ToolboxService
}

func NewToolboxApi(ToolboxService *ToolboxService) *ToolboxApi {
	return &ToolboxApi{
		ServerContext:  ToolboxService.ServerContext,
		ToolboxService: ToolboxService,
	}
}

var (
	// 工具 权限

	// Power 工具基本 权限
	Power          = base.AppendPower(&base.PowerAction{Action: "toolbox", Text: "工具箱", ShouldLogin: true, StandAlone: true})
	PowerList      = base.AppendPower(&base.PowerAction{Action: "toolbox_list", Text: "工具箱列表", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerCount     = base.AppendPower(&base.PowerAction{Action: "toolbox_count", Text: "工具箱统计", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerInsert    = base.AppendPower(&base.PowerAction{Action: "toolbox_insert", Text: "工具箱新增", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerUpdate    = base.AppendPower(&base.PowerAction{Action: "toolbox_update", Text: "工具箱修改", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerRename    = base.AppendPower(&base.PowerAction{Action: "toolbox_rename", Text: "工具箱重命名", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerDelete    = base.AppendPower(&base.PowerAction{Action: "toolbox_delete", Text: "工具箱删除", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerMoveGroup = base.AppendPower(&base.PowerAction{Action: "toolbox_move_group", Text: "工具箱分组", Parent: Power, ShouldLogin: true, StandAlone: true})

	PowerGroupList   = base.AppendPower(&base.PowerAction{Action: "toolbox_group_list", Text: "工具箱分组列表", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerGroupInsert = base.AppendPower(&base.PowerAction{Action: "toolbox_group_insert", Text: "工具箱分组新增", Parent: PowerGroupList, ShouldLogin: true, StandAlone: true})
	PowerGroupUpdate = base.AppendPower(&base.PowerAction{Action: "toolbox_group_update", Text: "工具箱分组修改", Parent: PowerGroupList, ShouldLogin: true, StandAlone: true})
	PowerGroupDelete = base.AppendPower(&base.PowerAction{Action: "toolbox_group_delete", Text: "工具箱分组删除", Parent: PowerGroupList, ShouldLogin: true, StandAlone: true})

	PowerOpen                = base.AppendPower(&base.PowerAction{Action: "toolbox_open", Text: "打开工具箱工具", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerUpdateOpenExtend    = base.AppendPower(&base.PowerAction{Action: "toolbox_update_open_extend", Text: "更新工具箱工具扩展", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerQueryOpens          = base.AppendPower(&base.PowerAction{Action: "toolbox_query_opens", Text: "查询打开的工具箱工具", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerGetOpen             = base.AppendPower(&base.PowerAction{Action: "toolbox_get_open", Text: "查询工具箱工具打开信息", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerClose               = base.AppendPower(&base.PowerAction{Action: "toolbox_close", Text: "工具箱工具关闭", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerOpenTab             = base.AppendPower(&base.PowerAction{Action: "toolbox_open_tab", Text: "工具箱工具打开Tab", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerQueryOpenTabs       = base.AppendPower(&base.PowerAction{Action: "toolbox_query_open_tabs", Text: "查询工具箱工具打开的Tab", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerCloseTab            = base.AppendPower(&base.PowerAction{Action: "toolbox_close_tab", Text: "工具箱工具关闭Tab", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerUpdateOpenTabExtend = base.AppendPower(&base.PowerAction{Action: "toolbox_update_open_tab_extend", Text: "更新工具箱工具Tab扩展", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerWork                = base.AppendPower(&base.PowerAction{Action: "toolbox_work", Text: "工具箱工具工作", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerDatabaseDownload    = base.AppendPower(&base.PowerAction{Action: "toolbox_database_download", Text: "工具箱数据库下载", Parent: Power, ShouldLogin: true, StandAlone: true})

	PowerQuickCommandQuery  = base.AppendPower(&base.PowerAction{Action: "toolbox_quickCommand_query", Text: "工具快速指令查询", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerQuickCommandInsert = base.AppendPower(&base.PowerAction{Action: "toolbox_quickCommand_insert", Text: "工具快速指令新增", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerQuickCommandUpdate = base.AppendPower(&base.PowerAction{Action: "toolbox_quickCommand_update", Text: "工具快速指令修改", Parent: Power, ShouldLogin: true, StandAlone: true})
	PowerQuickCommandDelete = base.AppendPower(&base.PowerAction{Action: "toolbox_quickCommand_delete", Text: "工具快速指令删除", Parent: Power, ShouldLogin: true, StandAlone: true})
)

func (this_ *ToolboxApi) GetApis() (apis []*base.ApiWorker) {
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/list"}, Power: PowerList, Do: this_.list})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/count"}, Power: PowerCount, Do: this_.count})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/insert"}, Power: PowerInsert, Do: this_.insert})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/update"}, Power: PowerUpdate, Do: this_.update})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/rename"}, Power: PowerRename, Do: this_.rename})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/delete"}, Power: PowerDelete, Do: this_.delete})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/moveGroup"}, Power: PowerMoveGroup, Do: this_.moveGroup})

	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/group/list"}, Power: PowerGroupList, Do: this_.listGroup})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/group/insert"}, Power: PowerGroupInsert, Do: this_.insertGroup})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/group/update"}, Power: PowerGroupUpdate, Do: this_.updateGroup})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/group/delete"}, Power: PowerGroupDelete, Do: this_.deleteGroup})

	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/work"}, Power: PowerWork, Do: this_.work})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/open"}, Power: PowerOpen, Do: this_.open})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/getOpen"}, Power: PowerGetOpen, Do: this_.getOpen})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/queryOpens"}, Power: PowerQueryOpens, Do: this_.queryOpens})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/close"}, Power: PowerClose, Do: this_.close})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/updateOpenExtend"}, Power: PowerUpdateOpenExtend, Do: this_.updateOpenExtend})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/queryOpenTabs"}, Power: PowerQueryOpenTabs, Do: this_.queryOpenTabs})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/openTab"}, Power: PowerOpenTab, Do: this_.openTab})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/closeTab"}, Power: PowerCloseTab, Do: this_.closeTab})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/updateOpenTabExtend"}, Power: PowerUpdateOpenTabExtend, Do: this_.updateOpenTabExtend})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/database/download"}, Power: PowerDatabaseDownload, Do: this_.databaseDownload, IsGet: true})

	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/quickCommand/query"}, Power: PowerQuickCommandQuery, Do: this_.queryQuickCommand})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/quickCommand/insert"}, Power: PowerQuickCommandInsert, Do: this_.insertQuickCommand})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/quickCommand/update"}, Power: PowerQuickCommandUpdate, Do: this_.updateQuickCommand})
	apis = append(apis, &base.ApiWorker{Apis: []string{"toolbox/quickCommand/delete"}, Power: PowerQuickCommandDelete, Do: this_.deleteQuickCommand})

	return
}

type QuickCommandType struct {
	Name  string `json:"name,omitempty"`
	Text  string `json:"text,omitempty"`
	Value int    `json:"value,omitempty"`
}

var (
	QuickCommandTypes []*QuickCommandType
)

func init() {
	QuickCommandTypes = append(QuickCommandTypes, &QuickCommandType{Name: "SSH Command", Text: "", Value: 1})
}

func GetQuickCommandTypes() []*QuickCommandType {
	return QuickCommandTypes
}
