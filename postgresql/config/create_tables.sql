--
-- Name: sys_func_role; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_func_role (
    role_id integer NOT NULL,
    function_id integer NOT NULL
) ;



--
-- Name: sys_func_role_type; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_func_role_type (
    role_type integer NOT NULL,
    function_id integer NOT NULL
) ;



--
-- Name: sys_function; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_function (
    function_id integer NOT NULL,
    function_code character varying(64) NOT NULL,
    function_name character varying(100) NOT NULL,
    status smallint,
    function_pid integer NOT NULL,
    function_type integer,
    url character varying(1024),
    remark character varying(256),
    icon_url character varying,
    menu_order integer
) ;



--
-- Name: sys_function_url; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_function_url (
    url_id integer NOT NULL,
    function_id integer NOT NULL,
    url character varying(1024) NOT NULL,
    visible character(1) DEFAULT 'Y'::bpchar
) ;

--
-- Name: sys_menu_tree; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_menu_tree (
    node_id integer NOT NULL,
    node_pid integer NOT NULL,
    node_name character varying(128) NOT NULL,
    node_url character varying(128) NOT NULL,
    node_title character varying(128),
    display_sn smallint,
    node_icon_url character varying(128),
    node_open character(1),
    visiable character(1) DEFAULT 'Y'::bpchar
) ;

--
-- Name: sys_prov_info; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_prov_info (
    prov_id integer,
    prov_name character varying(50),
    create_time timestamp without time zone,
    update_time timestamp without time zone
) ;



--
-- Name: sys_role; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_role (
    role_id integer NOT NULL,
    role_name character varying(300) NOT NULL,
    role_status integer NOT NULL,
    role_type integer,
    create_time timestamp without time zone DEFAULT now() NOT NULL,
    update_time timestamp without time zone,
    editable character(1) DEFAULT 'Y'::bpchar NOT NULL,
    remark character varying(256)
) ;

--
-- Name: sys_role_type; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_role_type (
    role_type integer NOT NULL,
    type_name character varying NOT NULL
) ;

--
-- Name: sys_user; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_user (
    user_id integer NOT NULL,
    user_code character varying(300) NOT NULL,
    user_pwd character varying(32) NOT NULL,
    recent_pwd character varying(200),
    status smallint NOT NULL,
    try_times smallint,
    pwd_expired_time date NOT NULL,
    last_login_time timestamp without time zone,
    fail_login_times integer
) ;

--
-- Name: sys_user_data_privilege; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_user_data_privilege (
    user_id integer NOT NULL,
    prov_code character varying(5) NOT NULL
) ;

--
-- Name: sys_user_info; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_user_info (
    user_id integer NOT NULL,
    user_name character varying(300) NOT NULL,
    org_id integer,
    email character varying(200),
    mobile character varying(20),
    phone character varying(20),
    address character varying(200),
    create_time timestamp without time zone DEFAULT now() NOT NULL,
    update_time timestamp without time zone,
    description character varying(3072),
    editable character(1) DEFAULT 'Y'::bpchar NOT NULL
) ;

--
-- Name: sys_user_role; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE sys_user_role (
    user_id integer NOT NULL,
    role_id integer NOT NULL
) ;

--
-- Name: widget; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE widget (
    id integer NOT NULL,
    user_id integer NOT NULL,
    class_id integer NOT NULL,
    title character varying(255) NOT NULL,
    remark character varying(255),
    seq integer NOT NULL,
    created timestamp without time zone NOT NULL,
    created_by integer NOT NULL,
    status character varying(2) NOT NULL,
    last_upd timestamp without time zone,
    last_upd_by integer
) ;

--
-- Name: TABLE public.widget ; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON TABLE public.widget  IS '控件信息表';


--
-- Name: COLUMN widget.user_id; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.user_id IS '用户ID';


--
-- Name: COLUMN widget.class_id; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.class_id IS '控件类型ID';


--
-- Name: COLUMN widget.title; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.title IS '标题';


--
-- Name: COLUMN widget.remark; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.remark IS '描述';


--
-- Name: COLUMN widget.seq; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.seq IS '控件排序';


--
-- Name: COLUMN widget.created; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.created IS '创建时间';


--
-- Name: COLUMN widget.created_by; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.created_by IS '创建人';


--
-- Name: COLUMN widget.status; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.status IS '状态，1-最小化 2-视窗 3-最大化';


--
-- Name: COLUMN widget.last_upd; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.last_upd IS '最后更新时间';


--
-- Name: COLUMN widget.last_upd_by; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget.last_upd_by IS '最后更新人';


--
-- Name: widget_class; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE widget_class (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    remark character varying(255),
    normal_url character varying(255) NOT NULL,
    detail_url character varying(255) NOT NULL,
    conf_url character varying(255),
    icon character varying(255) NOT NULL,
    is_configurable character varying(2) NOT NULL,
    created timestamp without time zone NOT NULL,
    created_by integer NOT NULL,
    status character varying(2) NOT NULL,
    last_upd timestamp without time zone,
    last_upd_by integer,
    function_id integer
) ;

--
-- Name: TABLE public.widget_class ; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON TABLE public.widget_class  IS '控件类型表';


--
-- Name: COLUMN widget_class.name; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.name IS '名称';


--
-- Name: COLUMN widget_class.remark; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.remark IS '描述';


--
-- Name: COLUMN widget_class.normal_url; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.normal_url IS '视窗状态内容URL';


--
-- Name: COLUMN widget_class.detail_url; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.detail_url IS '最大化状态内容URL';


--
-- Name: COLUMN widget_class.conf_url; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.conf_url IS '设置内容URL';


--
-- Name: COLUMN widget_class.icon; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.icon IS 'icon的URL';


--
-- Name: COLUMN widget_class.is_configurable; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.is_configurable IS '是否可配置，0-可以，1-不可以';


--
-- Name: COLUMN widget_class.created; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.created IS '创建时间';


--
-- Name: COLUMN widget_class.created_by; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.created_by IS '创建人';


--
-- Name: COLUMN widget_class.status; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.status IS '状态，0-有效，1-无效';


--
-- Name: COLUMN widget_class.last_upd; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.last_upd IS '最后更新时间';


--
-- Name: COLUMN widget_class.last_upd_by; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.last_upd_by IS '最后更新人';


--
-- Name: COLUMN widget_class.function_id; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_class.function_id IS '小窗口对应的权限ID';


--
-- Name: widget_param; Type: TABLE; Schema: public; Owner: aidns; Tablespace: 
--

CREATE TABLE widget_param (
    id integer NOT NULL,
    widget_id integer NOT NULL,
    param_name character varying(120) NOT NULL,
    value character varying(255) NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    created_by integer NOT NULL,
    status character varying(2) NOT NULL,
    last_upd timestamp without time zone DEFAULT now(),
    last_upd_by integer,
    remarks character varying(100)
) ;

--
-- Name: TABLE public.widget_param ; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON TABLE public.widget_param  IS '用户widget参数配置';


--
-- Name: COLUMN widget_param.id; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.id IS 'id';


--
-- Name: COLUMN widget_param.widget_id; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.widget_id IS 'widget id';


--
-- Name: COLUMN widget_param.param_name; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.param_name IS '参数名称';


--
-- Name: COLUMN widget_param.value; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.value IS '参数值';


--
-- Name: COLUMN widget_param.created; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.created IS '创建时间';


--
-- Name: COLUMN widget_param.created_by; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.created_by IS '创建人';


--
-- Name: COLUMN widget_param.status; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.status IS '状态：0 有效 1 无效';


--
-- Name: COLUMN widget_param.last_upd; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.last_upd IS '更新时间';


--
-- Name: COLUMN widget_param.last_upd_by; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.last_upd_by IS '更新人';


--
-- Name: COLUMN widget_param.remarks; Type: COMMENT; Schema: public; Owner: aidns
--

COMMENT ON COLUMN widget_param.remarks IS '备注';

--
-- Name: pk_menu_tree_id; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY sys_menu_tree
    ADD CONSTRAINT pk_menu_tree_id PRIMARY KEY (node_id);


--
-- Name: pk_sys_role_id; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY sys_role
    ADD CONSTRAINT pk_sys_role_id PRIMARY KEY (role_id);


--
-- Name: pk_sys_user_id; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY sys_user
    ADD CONSTRAINT pk_sys_user_id PRIMARY KEY (user_id);


--
-- Name: pk_url_id; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY sys_function_url
    ADD CONSTRAINT pk_url_id PRIMARY KEY (url_id);


--
-- Name: pk_widget; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY widget
    ADD CONSTRAINT pk_widget PRIMARY KEY (id);


--
-- Name: pk_widget_class; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY widget_class
    ADD CONSTRAINT pk_widget_class PRIMARY KEY (id);


--
-- Name: pk_widget_param; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY widget_param
    ADD CONSTRAINT pk_widget_param PRIMARY KEY (id);

--
-- Name: sys_function_pkey; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY sys_function
    ADD CONSTRAINT sys_function_pkey PRIMARY KEY (function_id);


--
-- Name: sys_user_role_id; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY sys_user_role
    ADD CONSTRAINT sys_user_role_id PRIMARY KEY (user_id, role_id);


--
-- Name: sys_user_role_id; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY sys_role_type
    ADD CONSTRAINT sys_user_role_id PRIMARY KEY (role_type);


--
-- Name: sys_user_role_id; Type: CONSTRAINT; Schema: public; Owner: aidns; Tablespace: 
--

ALTER TABLE ONLY sys_func_role_type
    ADD CONSTRAINT sys_user_role_id PRIMARY KEY (role_type, function_id);
